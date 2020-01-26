import numbers

import pandas as pd
import numpy as np

from vivarium.framework.state_machine import Machine
from vivarium_public_health.disease import DiseaseState, SusceptibleState, RateTransition
from vivarium_public_health.disease.model import VivariumError

import vivarium_csu_ltbi.globals as ltbi_globals


def wrap_data_getter(id):
    return {
        id: lambda _, builder: builder.data.load(
            f'sequela.{id}.prevalence'
        )
    }


class BetterDiseaseModel(Machine):
    """FIXME: This class used to extend DiseaseModel itself, but the population
              initializer has an additional dependency now. To facilitate
              declaring it, DiseaseModel itself is reproduced here.
              This is a hammer."""

    def __init__(self, cause, initial_state=None, get_data_functions=None, cause_type="cause", **kwargs):
        super().__init__(cause, **kwargs)
        self.cause = cause
        self.cause_type = cause_type

        if initial_state is not None:
            self.initial_state = initial_state.state_id
        else:
            self.initial_state = self._get_default_initial_state()

        self._get_data_functions = get_data_functions if get_data_functions is not None else {}

    @property
    def name(self):
        return f"disease_model.{self.cause}"

    def setup(self, builder):
        super().setup(builder)

        self.configuration_age_start = builder.configuration.population.age_start
        self.configuration_age_end = builder.configuration.population.age_end

        cause_specific_mortality_rate = self.load_cause_specific_mortality_rate_data(builder)
        self.cause_specific_mortality_rate = builder.lookup.build_table(cause_specific_mortality_rate,
                                                                        key_columns=['sex'],
                                                                        parameter_columns=['age', 'year'])
        builder.value.register_value_modifier('cause_specific_mortality_rate',
                                              self.adjust_cause_specific_mortality_rate,
                                              requires_columns=['age', 'sex'])

        builder.value.register_value_modifier('metrics', modifier=self.metrics)

        self.population_view = builder.population.get_view(['age', 'sex', self.state_column])
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=[self.state_column],
                                                 requires_columns=['age', 'sex'],
                                                 requires_values=['ltbi_susceptible_hiv.prevalence',
                                                                  'ltbi_positive_hiv.prevalence',
                                                                  'ltbi_susceptible_hiv.birth_prevalence',
                                                                  'ltbi_positive_hiv.birth_prevalence'],
                                                 requires_streams=[f'{self.state_column}_initial_states'])
        self.randomness = builder.randomness.get_stream(f'{self.state_column}_initial_states')

        builder.event.register_listener('time_step', self.on_time_step)
        builder.event.register_listener('time_step__cleanup', self.on_time_step_cleanup)

    def on_initialize_simulants(self, pop_data):
        population = self.population_view.subview(['age', 'sex']).get(pop_data.index)

        assert self.initial_state in {s.state_id for s in self.states}

        # FIXME: this is a hack to figure out whether or not we're at the simulation start based on the fact that the
        #  fertility components create this user data
        if pop_data.user_data['sim_state'] == 'setup':  # simulation start
            if self.configuration_age_start != self.configuration_age_end != 0:
                state_names, weights_bins = self.get_state_weights(pop_data.index, "prevalence")
            else:
                raise NotImplementedError('We do not currently support an age 0 cohort. '
                                          'configuration.population.age_start and configuration.population.age_end '
                                          'cannot both be 0.')

        else:  # on time step
            if pop_data.user_data['age_start'] == pop_data.user_data['age_end'] == 0:
                state_names, weights_bins = self.get_state_weights(pop_data.index, "birth_prevalence")
            else:
                state_names, weights_bins = self.get_state_weights(pop_data.index, "prevalence")

        if state_names and not population.empty:
            # only do this if there are states in the model that supply prevalence data
            population['sex_id'] = population.sex.apply({'Male': 1, 'Female': 2}.get)

            condition_column = self.assign_initial_status_to_simulants(population, state_names, weights_bins,
                                                                       self.randomness.get_draw(population.index))

            condition_column = condition_column.rename(columns={'condition_state': self.state_column})
        else:
            condition_column = pd.Series(self.initial_state, index=population.index, name=self.state_column)
        self.population_view.update(condition_column)

    def on_time_step(self, event):
        self.transition(event.index, event.time)

    def on_time_step_cleanup(self, event):
        self.cleanup(event.index, event.time)

    def load_cause_specific_mortality_rate_data(self, builder):
        if 'cause_specific_mortality_rate' not in self._get_data_functions:
            only_morbid = builder.data.load(f'cause.{self.cause}.restrictions')['yld_only']
            if only_morbid:
                csmr_data = 0
            else:
                csmr_data = builder.data.load(f"{self.cause_type}.{self.cause}.cause_specific_mortality_rate")
        else:
            csmr_data = self._get_data_functions['cause_specific_mortality_rate'](self.cause, builder)
        return csmr_data

    def adjust_cause_specific_mortality_rate(self, index, rate):
        return rate + self.cause_specific_mortality_rate(index)

    def _get_default_initial_state(self):
        susceptible_states = [s for s in self.states if isinstance(s, SusceptibleState)]
        if len(susceptible_states) != 1:
            raise DiseaseModelError("Disease model must have exactly one SusceptibleState.")
        return susceptible_states[0].state_id

    def get_state_weights(self, pop_index, prevalence_type):
        states = [s for s in self.states
                  if hasattr(s, f'{prevalence_type}') and getattr(s, f'{prevalence_type}') is not None]

        if not states:
            return states, None

        weights = [getattr(s, f'{prevalence_type}')(pop_index) for s in states]
        for w in weights:
            w.reset_index(inplace=True, drop=True)
        weights += ((1 - np.sum(weights, axis=0)), )

        weights = np.array(weights).T
        weights_bins = np.cumsum(weights, axis=1)

        state_names = [s.state_id for s in states] + [self.initial_state]

        return state_names, weights_bins

    @staticmethod
    def assign_initial_status_to_simulants(simulants_df, state_names, weights_bins, propensities):
        simulants = simulants_df[['age', 'sex']].copy()

        choice_index = (propensities.values[np.newaxis].T > weights_bins).sum(axis=1)
        initial_states = pd.Series(np.array(state_names)[choice_index], index=simulants.index)

        simulants.loc[:, 'condition_state'] = initial_states
        return simulants

    def to_dot(self):
        """Produces a ball and stick graph of this state machine.

        Returns
        -------
        `graphviz.Digraph`
            A ball and stick visualization of this state machine.
        """
        from graphviz import Digraph
        dot = Digraph(format='png')
        for state in self.states:
            if isinstance(state, TransientDiseaseState):
                dot.node(state.state_id, style='dashed', color='orange')
            elif isinstance(state, SusceptibleState):
                dot.node(state.state_id, color='green')
            else:
                dot.node(state.state_id, color='orange')
            for transition in state.transition_set:
                if transition._active_index is not None:  # Transition is a triggered transition
                    dot.attr('edge', style='bold')
                else:
                    dot.attr('edge', style='plain')

                if isinstance(transition, RateTransition):
                    dot.edge(state.state_id, transition.output_state.state_id, transition.label(), color='blue')
                elif isinstance(transition, ProportionTransition):
                    dot.edge(state.state_id, transition.output_state.state_id, transition.label(), color='purple')
                else:
                    dot.edge(state.state_id, transition.output_state.state_id, transition.label(), color='black')

            if state.transition_set.allow_null_transition:
                if hasattr(state, '_dwell_time'):
                    if isinstance(state._dwell_time, numbers.Number):
                        if state._dwell_time != 0:
                            label = "dwell_time: {}".format(state._dwell_time)
                            dot.edge(state.state_id, state.state_id, label, style='dotted')
                        else:
                            dot.edge(state.state_id, state.state_id, style='plain')
                    else:
                        dot.edge(state.state_id, state.state_id, style='dotted')
        return dot

    def metrics(self, index, metrics):
        population = self.population_view.get(index, query="alive == 'alive'")
        prevalent_cases = (population[self.state_column] != 'susceptible_to_' + self.state_column).sum()
        metrics[self.state_column + '_prevalent_cases_at_sim_end'] = prevalent_cases
        return metrics

    def metrics(self, index, metrics):
        """Suppress unnecessary columns."""
        return metrics


class BetterDiseaseState(DiseaseState):

    def setup(self, builder):
        super().setup(builder)

        # FIXME: Our goal is to use a pipeline to source prevalence and birth
        #        prevalence rather than a lookup table because the pipeline
        #        will allow us to intervene on those values to induce
        #        correlation with household TB exposure. The bits that rely only
        #        prevalence and birth prevalence to initialize population are
        #        nestled in complex functions inside disease state. But, lookup
        #        tables and pipelines have the same interface, so we can do a
        #        bait and switch. We will rename the lookup tables and then
        #        define attributes with the expected names that are pipelines
        #        sourcing the lookup tables.

        # re-assign lookup tables
        self._prevalence = self.prevalence
        self._birth_prevalence = self.birth_prevalence

        # Overwrite the lookup table attributes with pipelines
        self.prevalence = builder.value.register_value_producer(
            f'{self.state_id}.prevalence',
            source=self._prevalence,
            requires_columns=['age', 'sex']
        )
        self.birth_prevalence = builder.value.register_value_producer(
            f'{self.state_id}.birth_prevalence',
            source=self._birth_prevalence,
            requires_columns=['sex']
        )

    def add_transition(self, output, source_data_type=None, get_data_functions=None, **kwargs):
        if get_data_functions == None:
            get_data_functions = {'transition_rate': lambda cause, builder: builder.data.load(
                f'sequela.{self.cause}_to_{cause}.transition_rate')}
        t = BetterRateTransition(self, output, get_data_functions, **kwargs)
        self.transition_set.append(t)
        return t

    def metrics(self, index, metrics):
        """Suppress unnecessary columns."""
        return metrics


class BetterSusceptibleState(SusceptibleState):

    def __init__(self, cause, *args, **kwargs):
        # skip the initializer that adds the redundant prefix
        super(SusceptibleState, self).__init__(cause, *args, name_prefix='', **kwargs)

    def add_transition(self, output, source_data_type=None, get_data_functions=None, **kwargs):
        if get_data_functions == None:
            get_data_functions = {'transition_rate': lambda cause, builder: builder.data.load(
                f'sequela.{self.cause}_to_{cause}.transition_rate')}
        t = BetterRateTransition(self, output, get_data_functions, **kwargs)
        self.transition_set.append(t)
        return t

    def metrics(self, index, metrics):
        """Suppress unnecessary columns."""
        return metrics


class BetterRateTransition(RateTransition):
    def load_transition_rate_data(self, builder):
        if 'transition_rate' in self._get_data_functions:
            rate_data = self._get_data_functions['transition_rate'](self.output_state.cause, builder)
            pipeline_name = f'{self.input_state.state_id}_to_{self.output_state.state_id}.transition_rate'
        else:
            raise ValueError("No valid data functions supplied.")
        return rate_data, pipeline_name


def wrapped_builder_getter(id):
    return {
        id: lambda _, builder: builder.data.load(
            f'sequela.{id}.prevalence'
        )
    }


def wrapped_birth_prevalence_getter(id):
    def _load_birth_prev(_, builder):
        prev = builder.data.load(f'sequela.{id}.prevalence')
        prev = prev.loc[prev.age_start == 0]
        prev = prev.drop(['age_start', 'age_end'], axis=1)
        return prev

    return {
        'birth_prevalence': _load_birth_prev
    }


def get_disease_state(id):
    ds = BetterDiseaseState(id, cause_type='sequela', get_data_functions=wrapped_builder_getter(id))
    ds.allow_self_transitions()
    return ds


def TuberculosisAndHIV():
    # the non-disease state
    susceptible = BetterSusceptibleState(ltbi_globals.SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV)
    susceptible.allow_self_transitions()

    # the 'disease 'states
    ltbi_susceptible_hiv = get_disease_state(ltbi_globals.LTBI_SUSCEPTIBLE_HIV)
    ltbi_susceptible_hiv._get_data_functions.update(wrapped_birth_prevalence_getter(ltbi_globals.LTBI_SUSCEPTIBLE_HIV))

    activetb_susceptible_hiv = get_disease_state(ltbi_globals.ACTIVETB_SUSCEPTIBLE_HIV)

    susceptible_tb_positive_hiv = get_disease_state(ltbi_globals.SUSCEPTIBLE_TB_POSITIVE_HIV)

    ltbi_positive_hiv = get_disease_state(ltbi_globals.LTBI_POSITIVE_HIV)
    ltbi_positive_hiv._get_data_functions.update(wrapped_birth_prevalence_getter(ltbi_globals.LTBI_POSITIVE_HIV))

    activetb_positive_hiv = get_disease_state(ltbi_globals.ACTIVETB_POSITIVE_HIV)

    # the transitions
    susceptible.add_transition(ltbi_susceptible_hiv)
    susceptible_tb_positive_hiv.add_transition(ltbi_positive_hiv)

    susceptible.add_transition(susceptible_tb_positive_hiv)

    ltbi_susceptible_hiv.add_transition(activetb_susceptible_hiv)
    ltbi_susceptible_hiv.add_transition(ltbi_positive_hiv)

    ltbi_positive_hiv.add_transition(activetb_positive_hiv)

    activetb_susceptible_hiv.add_transition(susceptible)
    activetb_susceptible_hiv.add_transition(activetb_positive_hiv)

    activetb_positive_hiv.add_transition(susceptible_tb_positive_hiv)

    return BetterDiseaseModel(ltbi_globals.TUBERCULOSIS_AND_HIV,
                              states=[susceptible,
                                      ltbi_susceptible_hiv,
                                      activetb_susceptible_hiv,
                                      susceptible_tb_positive_hiv,
                                      ltbi_positive_hiv,
                                      activetb_positive_hiv
                                      ]
                              )

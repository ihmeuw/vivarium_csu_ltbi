from vivarium_public_health.disease import DiseaseState, DiseaseModel, SusceptibleState, RateTransition

import vivarium_csu_ltbi.globals as ltbi_globals


def wrap_data_getter(id):
    return {
        id: lambda _, builder: builder.data.load(
            f'sequela.{id}.prevalence'
        )
    }


class BetterDiseaseModel(DiseaseModel):

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
            requires_columns=['year', 'age', 'sex']
        )
        self.birth_prevalence = builder.value.register_value_producer(
            f'{self.state_id}.birth_prevalence',
            source=self._birth_prevalence,
            requires_columns=['year', 'sex']
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
    # ltbi_susceptible_hiv._get_data_functions.update(wrapped_birth_prevalence_getter(ltbi_globals.LTBI_SUSCEPTIBLE_HIV))

    activetb_susceptible_hiv = get_disease_state(ltbi_globals.ACTIVETB_SUSCEPTIBLE_HIV)

    susceptible_tb_positive_hiv = get_disease_state(ltbi_globals.SUSCEPTIBLE_TB_POSITIVE_HIV)

    ltbi_positive_hiv = get_disease_state(ltbi_globals.LTBI_POSITIVE_HIV)
    # ltbi_positive_hiv._get_data_functions.update(wrapped_birth_prevalence_getter(ltbi_globals.LTBI_POSITIVE_HIV))

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

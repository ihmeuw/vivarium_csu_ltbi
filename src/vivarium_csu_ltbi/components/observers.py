import pandas as pd

from vivarium_public_health.metrics import (DiseaseObserver, MortalityObserver, DisabilityObserver)
from vivarium_public_health.metrics.utilities import (get_output_template, get_disease_event_counts, QueryString,
                                                      get_group_counts, to_years, get_years_lived_with_disability,
                                                      get_person_time, get_deaths, get_years_of_life_lost)

from vivarium_csu_ltbi import globals as ltbi_globals


class HouseholdTuberculosisDiseaseObserver(DiseaseObserver):

    def __init__(self, disease):
        super().__init__(disease)

    def setup(self, builder):
        super().setup(builder)

        self.total_population = {}

        disease_component = builder.components.get_component(f"disease_model.{ltbi_globals.TUBERCULOSIS_AND_HIV}")
        self.states = [state.name.split('.')[1] for state in disease_component.states]

        self.previous_state_column = f'previous_{self.disease}'
        builder.population.initializes_simulants(self.initialize_previous_state,
                                                 creates_columns=[self.previous_state_column])

        # This overrides an attribute in the parent
        columns_required = ['alive', f'{self.disease}', f'{self.disease}_event_time', self.previous_state_column]
        for state in self.states:
            columns_required.append(f'{state}_event_time')

        if self.config.by_age:
            columns_required += ['age']
        if self.config.by_sex:
            columns_required += ['sex']
        self.population_view = builder.population.get_view(columns_required)

        self.household_tb_exposure = builder.value.get_value(f'{ltbi_globals.HOUSEHOLD_TUBERCULOSIS}.exposure')

    def initialize_previous_state(self, pop_data):
        self.population_view.update(pd.Series('', index=pop_data.index, name=self.previous_state_column))

    @staticmethod
    def get_state_person_time(pop, config, disease, state, current_year, step_size, age_bins):
        """Custom person time getter that handles state column name assumptions"""
        base_key = get_output_template(**config).substitute(measure=f'{state}_person_time',
                                                            year=current_year)
        base_filter = QueryString(f'alive == "alive" and {disease} == "{state}"')
        person_time = get_group_counts(pop, base_filter, base_key, config, age_bins,
                                       aggregate=lambda x: len(x) * to_years(step_size))
        return person_time

    @staticmethod
    def get_state_prevalent_cases(pop, config, disease, state, event_time, age_bins):
        config = config.copy()
        config['by_year'] = True  # This is always an annual point estimate
        base_key = get_output_template(**config).substitute(measure=f'{state}_prevalent_cases', year=event_time.year)
        base_filter = QueryString(f'alive == "alive" and {disease} == "{state}"')
        return get_group_counts(pop, base_filter, base_key, config, age_bins)

    @staticmethod
    def get_population(pop, config, event_time, age_bins):
        config = config.copy()
        config['by_year'] = True  # This is always an annual point estimate
        base_key = get_output_template(**config).substitute(measure=f'population_point_estimate', year=event_time.year)
        base_filter = QueryString(f'alive == "alive"')
        return get_group_counts(pop, base_filter, base_key, config, age_bins)

    def on_time_step_prepare(self, event):
        pop = self.population_view.get(event.index)
        pop_exposure_category = self.household_tb_exposure(event.index)

        for category in ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_CATEGORIES:
            exposure_state = ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_MAP[category]
            pop_in_category = pop.loc[pop_exposure_category == category]
            for state in self.states:
                state_person_time_this_step = self.get_state_person_time(pop_in_category, self.config.to_dict(),
                                                                         self.disease, state, self.clock().year,
                                                                         event.step_size, self.age_bins)
                state_person_time_this_step = {f'{k}_{exposure_state}': v for k, v in state_person_time_this_step.items()}
                self.person_time.update(state_person_time_this_step)

        if self.should_sample(event.time):
            for category in ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_CATEGORIES:
                exposure_state = ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_MAP[category]
                pop_in_category = pop.loc[pop_exposure_category == category]

                point_population = self.get_population(pop_in_category, self.config.to_dict(),
                                                       event.time, self.age_bins)
                point_population = {f'{k}_{exposure_state}': v for k, v in point_population.items()}
                self.total_population.update(point_population)

                for state in self.states:
                    state_point_prevalence = self.get_state_prevalent_cases(pop_in_category, self.config.to_dict(),
                                                                            self.disease, state, event.time,
                                                                            self.age_bins)
                    state_point_prevalence = {f'{k}_{exposure_state}': v for k, v in state_point_prevalence.items()}
                    self.prevalence.update(state_point_prevalence)

        # This enables tracking of transitions between states
        prior_state_pop = self.population_view.get(event.index)
        prior_state_pop[self.previous_state_column] = prior_state_pop[self.disease]
        self.population_view.update(prior_state_pop)

    def on_collect_metrics(self, event):
        pop = self.population_view.get(event.index)
        pop_exposure_category = self.household_tb_exposure(event.index)

        for category in ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_CATEGORIES:
            exposure_state = ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_MAP[category]
            pop_in_category = pop.loc[(pop_exposure_category == category)]

            for transition in ltbi_globals.HIV_TB_TRANSITIONS:
                from_state, to_state = transition.split('_to_')
                event_this_step = ((pop_in_category[f'{to_state}_event_time'] == self.clock())
                                   & (pop_in_category[self.previous_state_column] == from_state))
                transitioned_pop = pop_in_category.loc[event_this_step]
                base_key = get_output_template(**self.config.to_dict()).substitute(measure=f'{transition}_event_count',
                                                                                   year=event.time.year)
                base_filter = QueryString('')
                transition_count = get_group_counts(transitioned_pop, base_filter, base_key,
                                                    self.config.to_dict(), self.age_bins)
                transition_count = {f"{k}_{exposure_state}": v for k, v in transition_count.items()}
                self.counts.update(transition_count)

    def metrics(self, index, metrics):
        metrics = super().metrics(index, metrics)
        metrics.update(self.total_population)
        return metrics


class HouseholdTuberculosisMortalityObserver(MortalityObserver):

    def __init__(self):
        super().__init__()

    def setup(self, builder):
        super().setup(builder)
        self.household_tb_exposure = builder.value.get_value(f'{ltbi_globals.HOUSEHOLD_TUBERCULOSIS}.exposure')

    def metrics(self, index, metrics):
        pop = self.population_view.get(index)
        pop.loc[pop.exit_time.isnull(), 'exit_time'] = self.clock()

        exposure_category = self.household_tb_exposure(index)

        measure_getters = (
            (get_person_time, ()),
            (get_deaths, (ltbi_globals.CAUSE_OF_DEATH_STATES,)),
            (get_years_of_life_lost, (self.life_expectancy, ltbi_globals.CAUSE_OF_DEATH_STATES)),
        )

        for category in ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_CATEGORIES:
            exposure_state = ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_MAP[category]
            category_pop = pop.loc[exposure_category == category]
            base_args = (category_pop, self.config.to_dict(), self.start_time, self.clock(), self.age_bins)

            for measure_getter, extra_args in measure_getters:
                measure_data = measure_getter(*base_args, *extra_args)
                measure_data = {f'{k}_{exposure_state}': v for k, v in measure_data.items()}
                metrics.update(measure_data)

        the_living = pop[(pop.alive == 'alive') & pop.tracked]
        the_dead = pop[pop.alive == 'dead']
        metrics['years_of_life_lost'] = self.life_expectancy(the_dead.index).sum()
        metrics['total_population_living'] = len(the_living)
        metrics['total_population_dead'] = len(the_dead)

        return metrics


class HouseholdTuberculosisDisabilityObserver(DisabilityObserver):

    def __init__(self):
        super().__init__()

    def setup(self, builder):
        super().setup(builder)
        self.household_tb_exposure = builder.value.get_value(f'{ltbi_globals.HOUSEHOLD_TUBERCULOSIS}.exposure')

    def on_time_step_prepare(self, event):
        pop = self.population_view.get(event.index, query='tracked == True and alive == "alive"')

        self.update_metrics(pop)

        pop.loc[:, 'years_lived_with_disability'] += self.disability_weight(pop.index)
        self.population_view.update(pop)

    def update_metrics(self, pop):
        exposure_category = self.household_tb_exposure(pop.index)

        for category in ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_CATEGORIES:
            exposure_state = ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_MAP[category]
            category_pop = pop.loc[exposure_category == category]
            ylds_this_step = get_years_lived_with_disability(category_pop, self.config.to_dict(),
                                                             self.clock().year, self.step_size(),
                                                             self.age_bins, self.disability_weight_pipelines,
                                                             self.causes)
            ylds_this_step = {f'{k}_{exposure_state}': v for k, v in ylds_this_step.items()}
            self.years_lived_with_disability.update(ylds_this_step)

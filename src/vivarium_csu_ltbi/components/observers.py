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

        disease_component = builder.components.get_component(f"disease_model.{ltbi_globals.TUBERCULOSIS_AND_HIV}")
        self.states = [state.name.split('.')[1] for state in disease_component.states]

        self.previous_state_column = f'previous_{self.disease}'
        builder.population.initializes_simulants(self.initialize_previous_state,
                                                 requires_columns=['alive'],
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
        population = self.population_view.subview(['alive']).get(pop_data.index)
        self.population_view.update(pd.Series('', index=population.index, name=self.previous_state_column))

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
                state_person_time_this_step = self.fix_susceptible_state_name(state, state_person_time_this_step)
                state_person_time_this_step = {f'{k}_{exposure_state}': v for k, v in state_person_time_this_step.items()}
                self.person_time.update(state_person_time_this_step)

        if self.should_sample(event.time):
            for category in ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_CATEGORIES:
                exposure_state = ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_MAP[category]
                pop_in_category = pop.loc[pop_exposure_category == category]
                for state in self.states:
                    state_point_prevalence = self.get_state_prevalent_cases(pop_in_category, self.config.to_dict(),
                                                                            self.disease, state, event.time,
                                                                            self.age_bins)
                    state_point_prevalence = self.fix_susceptible_state_name(state, state_point_prevalence)
                    state_point_prevalence = {f'{k}_{exposure_state}': v for k, v in state_point_prevalence.items()}
                    self.prevalence.update(state_point_prevalence)

        # This enables tracking of transitions between states
        prior_state_pop = self.population_view.get(event.index)
        prior_state_pop[self.previous_state_column] = prior_state_pop[self.disease]
        self.population_view.update(prior_state_pop)

    @staticmethod
    def fix_susceptible_state_name(state: str, data: dict):
        """Manually fixing an odd state name that is unavoidable from the disease model."""
        if state == f'susceptible_to_{ltbi_globals.SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV}':
            data = {f"{k.replace('susceptible_to_', '')}": v for k, v in data.items()}
        return data

    def on_collect_metrics(self, event):
        pop = self.population_view.get(event.index)
        pop_exposure_category = self.household_tb_exposure(event.index)

        for category in ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_CATEGORIES:
            exposure_state = ltbi_globals.HOUSEHOLD_TUBERCULOSIS_EXPOSURE_MAP[category]
            for state in self.states:
                pop_in_state = pop.loc[(pop_exposure_category == category) & (pop[self.disease] == state)]
                # we must iterate over every state even if we know only a subset are present because we must have
                # the same columns between simulations
                for previous_state in self.states:
                    if previous_state == state:
                        continue
                    pop_in_state_and_from_state = pop_in_state.loc[pop_in_state[self.previous_state_column] ==
                                                                   previous_state]
                    state_counts = get_disease_event_counts(pop_in_state_and_from_state, self.config.to_dict(),
                                                            state, event.time, self.age_bins)
                    state_counts = {f"{k}_from_{previous_state}_{exposure_state}": v for k, v in state_counts.items()}
                    state_counts = self.fix_susceptible_state_name(state, state_counts)
                    self.counts.update(state_counts)


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
            (get_deaths, (self.causes,)),
            (get_years_of_life_lost, (self.life_expectancy, self.causes)),
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

from vivarium_public_health.metrics import (DiseaseObserver, MortalityObserver,
                                            DisabilityObserver)
from vivarium_public_health.metrics.utilities import (get_disease_event_counts, get_years_lived_with_disability,
                                                      get_person_time, get_deaths, get_years_of_life_lost)


class HouseholdTuberculosisDiseaseObserver(DiseaseObserver):

    def __init__(self, disease):
        super().__init__(disease)

    def setup(self, builder):
        super().setup(builder)
        self.household_tb_exposure = builder.value.get_value('household_tuberculosis.exposure')

    def on_collect_metrics(self, event):
        pop = self.population_view.get(event.index)
        exposure_category = self.household_tb_exposure(event.index)

        exposed = pop.loc[exposure_category == 'cat1']
        unexposed = pop.loc[exposure_category == 'cat2']

        exposed_disease_events_this_step = get_disease_event_counts(exposed, self.config.to_dict(), self.disease,
                                                                    event.time, self.age_bins)
        exposed_disease_events_this_step = {f'{k}_exposed_to_hhtb': v for k, v in exposed_disease_events_this_step.items()}
        self.counts.update(exposed_disease_events_this_step)

        unexposed_disease_events_this_step = get_disease_event_counts(unexposed, self.config.to_dict(), self.disease,
                                                                      event.time, self.age_bins)
        unexposed_disease_events_this_step = {f'{k}_unexposed_to_hhtb': v for k, v in unexposed_disease_events_this_step.items()}
        self.counts.update(unexposed_disease_events_this_step)


class HouseholdTuberculosisMortalityObserver(MortalityObserver):

    def __init__(self):
        super().__init__()

    def setup(self, builder):
        super().setup(builder)
        self.household_tb_exposure = builder.value.get_value('household_tuberculosis.exposure')

    def metrics(self, index, metrics):
        pop = self.population_view.get(index)
        pop.loc[pop.exit_time.isnull(), 'exit_time'] = self.clock()

        exposure_category = self.household_tb_exposure(index)

        exposed = pop.loc[exposure_category == 'cat1']
        unexposed = pop.loc[exposure_category == 'cat2']

        exposed_person_time = get_person_time(exposed, self.config.to_dict(), self.start_time, self.clock(),
                                              self.age_bins)
        exposed_person_time = {f'{k}_exposed_to_hhtb': v for k, v in exposed_person_time.items()}
        exposed_deaths = get_deaths(exposed, self.config.to_dict(), self.start_time, self.clock(), self.age_bins,
                                    self.causes)
        exposed_deaths = {f'{k}_exposed_to_hhtb': v for k, v in exposed_deaths.items()}
        exposed_ylls = get_years_of_life_lost(exposed, self.config.to_dict(), self.start_time, self.clock(),
                                              self.age_bins, self.life_expectancy, self.causes)
        exposed_ylls = {f'{k}_exposed_to_hhtb': v for k, v in exposed_ylls.items()}

        metrics.update(exposed_person_time)
        metrics.update(exposed_deaths)
        metrics.update(exposed_ylls)

        unexposed_person_time = get_person_time(unexposed, self.config.to_dict(), self.start_time, self.clock(),
                                                self.age_bins)
        unexposed_person_time = {f'{k}_unexposed_to_hhtb': v for k, v in unexposed_person_time.items()}
        unexposed_deaths = get_deaths(unexposed, self.config.to_dict(), self.start_time, self.clock(), self.age_bins,
                                    self.causes)
        unexposed_deaths = {f'{k}_unexposed_to_hhtb': v for k, v in unexposed_deaths.items()}
        unexposed_ylls = get_years_of_life_lost(unexposed, self.config.to_dict(), self.start_time, self.clock(),
                                              self.age_bins, self.life_expectancy, self.causes)
        unexposed_ylls = {f'{k}_unexposed_to_hhtb': v for k, v in unexposed_ylls.items()}

        metrics.update(unexposed_person_time)
        metrics.update(unexposed_deaths)
        metrics.update(unexposed_ylls)

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
        self.household_tb_exposure = builder.value.get_value('household_tuberculosis.exposure')

    def on_time_step_prepare(self, event):
        pop = self.population_view.get(event.index, query='tracked == True and alive == "alive"')
        exposure_category = self.household_tb_exposure(event.index)

        exposed = pop.loc[exposure_category == 'cat1']
        unexposed = pop.loc[exposure_category == 'cat2']

        exposed_ylds_this_step = get_years_lived_with_disability(exposed, self.config.to_dict(),
                                                                 self.clock().year, self.step_size(),
                                                                 self.age_bins, self.disability_weight_pipelines,
                                                                 self.causes)
        exposed_ylds_this_step = {f'{k}_exposed_to_httb': v for k, v in exposed_ylds_this_step.items()}
        self.years_lived_with_disability.update(exposed_ylds_this_step)

        unexposed_ylds_this_step = get_years_lived_with_disability(unexposed, self.config.to_dict(),
                                                                   self.clock().year, self.step_size(),
                                                                   self.age_bins, self.disability_weight_pipelines,
                                                                   self.causes)
        unexposed_ylds_this_step = {f'{k}_unexposed_to_httb': v for k, v in unexposed_ylds_this_step.items()}
        self.years_lived_with_disability.update(unexposed_ylds_this_step)

        pop.loc[:, 'years_lived_with_disability'] += self.disability_weight(pop.index)
        self.population_view.update(pop)

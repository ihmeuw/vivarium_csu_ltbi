import numpy as np
import pandas as pd

from vivarium_csu_ltbi import globals as ltbi_globals


# noinspection PyAttributeOutsideInit
class LTBITreatmentCoverage:

    @property
    def name(self):
        return 'ltbi_treatment_coverage'

    def setup(self, builder):
        self.clock = builder.time.clock()

        self.treatment_stream = builder.randomness.get_stream(f'{self.name}.treatment_selection')
        self.adherence_stream = builder.randomness.get_stream(f'{self.name}.adherence_propensity')

        self.household_tb_exposure = builder.value.get_value('household_tuberculosis.exposure')

        adherence_data = builder.data.load("ltbi_treatment.adherence")
        self.adherence = builder.lookup.build_table(adherence_data,
                                                    parameter_columns=['age', 'year'],
                                                    key_columns=['sex', 'treatment_type'],
                                                    value_columns=['value'])

        coverage_data = self.get_coverage_data(builder)
        _coverage_raw = builder.lookup.build_table(coverage_data, parameter_columns=['age', 'year'],
                                                   key_columns=['sex'],
                                                   value_columns=['with_hiv_6H', 'with_hiv_3HP',
                                                                  'under_five_hhtb_6H', 'under_five_hhtb_3HP'])
        self.coverage_raw = builder.value.register_value_producer('ltbi_treatment.data', source=_coverage_raw)

        self.coverage_filtered = builder.value.register_value_producer('ltbi_treatment.coverage',
                                                                       source=self.get_coverage,
                                                                       requires_columns=['age', ltbi_globals.TUBERCULOSIS_AND_HIV],
                                                                       requires_values=['household_tuberculosis.exposure'],
                                                                       preferred_post_processor=self.enforce_not_eligible)

        self._ltbi_treatment_status = pd.Series()
        self.ltbi_treatment_status = builder.value.register_value_producer(
            'ltbi_treatment.exposure',
            source=lambda index: self._ltbi_treatment_status[index],
            requires_streams=[f'{self.name}.adherence_propensity']
        )

        self.columns_created = ['treatment_date', 'treatment_type', 'adherence_propensity', 'treatment_propensity']
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 requires_columns=[],
                                                 creates_columns=self.columns_created)

        self.population_view = builder.population.get_view([ltbi_globals.TUBERCULOSIS_AND_HIV, 'age', 'sex', 'alive'] +
                                                           self.columns_created,
                                                           query="alive == 'alive'")

        builder.event.register_listener('time_step__prepare', self.on_time_step_prepare)

    def on_initialize_simulants(self, pop_data):

        self._ltbi_treatment_status = self._ltbi_treatment_status.append(pd.Series('untreated', index=pop_data.index))
        initialized = pd.DataFrame({'treatment_date': pd.NaT,
                                    'treatment_type': 'untreated',
                                    'adherence_propensity': self.adherence_stream.get_draw(pop_data.index),
                                    'treatment_propensity': self.treatment_stream.get_draw(pop_data.index)},
                                   index=pop_data.index)
        self.population_view.update(initialized)

    def on_time_step_prepare(self, event):
        pop = self.population_view.get(event.index, query="treatment_type == 'untreated'")

        coverage = self.coverage_filtered(event.index)
        p = np.array(coverage)
        p = p / p.sum(axis=1, keepdims=True)
        p_bins = np.cumsum(p, axis=1)
        draw = pop['treatment_propensity']
        choice_index = (draw.values[np.newaxis].T > p_bins).sum(axis=1)
        treatment_type = pd.Series(np.array(coverage.columns)[choice_index], index=pop.index)
        newly_treated = treatment_type != 'untreated'  # those actually selected for treatment
        pop.loc[newly_treated, 'treatment_type'] = treatment_type

        pop.loc[newly_treated, 'treatment_date'] = self.clock()
        self.population_view.update(pop)

        are_adherent = pop.loc[newly_treated, 'adherence_propensity'] <= self.adherence(pop.loc[newly_treated].index)
        treatment_status = treatment_type.loc[newly_treated].copy()
        treatment_status.loc[are_adherent] += '_adherent'
        treatment_status.loc[~are_adherent] += '_nonadherent'

        self._ltbi_treatment_status.update(pd.Series(treatment_status, index=treatment_status.index))

    def get_coverage(self, index):
        pop = self.population_view.get(index, query="treatment_type == 'untreated'")

        coverage = pd.DataFrame(data={'3HP': 0.0, '6H': 0.0, 'untreated': 1.0},
                                index=pop.index)

        # Generate bit masks for our group conditions
        with_hiv = self.get_hiv_positive_subgroup(pop)
        under_five_hhtb = self.get_under_five_hhtb_subgroup(pop)
        in_both_groups = with_hiv & under_five_hhtb

        # this is where the intervention intercedes
        raw_coverage = self.coverage_raw(pop.index)

        coverage.loc[with_hiv, '3HP'] = raw_coverage.loc[with_hiv, 'with_hiv_3HP']
        coverage.loc[with_hiv, '6H'] = raw_coverage.loc[with_hiv, 'with_hiv_6H']
        coverage.loc[under_five_hhtb, '3HP'] = raw_coverage.loc[under_five_hhtb, 'under_five_hhtb_3HP']
        coverage.loc[under_five_hhtb, '6H'] = raw_coverage.loc[under_five_hhtb, 'under_five_hhtb_6H']
        coverage.loc[in_both_groups,  '3HP'] = 1. - ((1. - raw_coverage.loc[with_hiv, 'with_hiv_3HP'])
                                                     * (1. - raw_coverage.loc[under_five_hhtb, 'under_five_hhtb_3HP']))
        coverage.loc[in_both_groups, '6H'] = 1. - ((1. - raw_coverage.loc[with_hiv, 'with_hiv_6H'])
                                                   * (1. - raw_coverage.loc[under_five_hhtb, 'under_five_hhtb_6H']))

        coverage['untreated'] = 1. - coverage['6H'] - coverage['3HP']

        return coverage

    def enforce_not_eligible(self, data, timestep):
        pop = self.population_view.subview(['age', ltbi_globals.TUBERCULOSIS_AND_HIV]).get(data.index)

        with_hiv = self.get_hiv_positive_subgroup(pop)
        under_five_hhtb = self.get_under_five_hhtb_subgroup(pop)

        data.loc[~with_hiv & ~under_five_hhtb, '3HP'] = 0.0
        data.loc[~with_hiv & ~under_five_hhtb, '6H'] = 0.0
        data.loc[~with_hiv & ~under_five_hhtb, 'untreated'] = 1.0

        return data

    @staticmethod
    def get_hiv_positive_subgroup(pop):
        """Returns a bit mask of simulants in the treatment subgroup that is
        HIV+ and does not have active TB. The population is already filtered
        to untreated."""
        with_hiv = ((pop[ltbi_globals.TUBERCULOSIS_AND_HIV] == ltbi_globals.ACTIVETB_POSITIVE_HIV)
                    | (pop[ltbi_globals.TUBERCULOSIS_AND_HIV] == ltbi_globals.LTBI_POSITIVE_HIV)
                    | (pop[ltbi_globals.TUBERCULOSIS_AND_HIV] == ltbi_globals.SUSCEPTIBLE_TB_POSITIVE_HIV))

        return with_hiv

    def get_under_five_hhtb_subgroup(self, pop):
        """Returns a bit mask of simulants in the treatment subgroup that is
        under 5, exposed to household TB, and does not have active TB. The
        population is already filtered to untreated."""
        age_five_and_under = pop['age'] <= 5.0
        exposed_hhtb = self.household_tb_exposure(pop.index) == 'cat1'
        no_active_tb = ((pop[ltbi_globals.TUBERCULOSIS_AND_HIV] != ltbi_globals.ACTIVETB_POSITIVE_HIV)
                        & (pop[ltbi_globals.TUBERCULOSIS_AND_HIV] != ltbi_globals.ACTIVETB_SUSCEPTIBLE_HIV))
        return age_five_and_under & exposed_hhtb & no_active_tb

    @staticmethod
    def get_coverage_data(builder):
        coverage_data = builder.data.load("risk_factor.ltbi_treatment.coverage")
        coverage_data['treatment_group'] = coverage_data['treatment_subgroup'] + '_' + coverage_data['treatment_type']
        coverage_data = coverage_data.drop(['treatment_subgroup', 'treatment_type'], axis=1)
        key_cols = ['sex', 'age_start', 'age_end', 'year_start', 'year_end']
        coverage_data = coverage_data.pivot_table(index=key_cols, columns=['treatment_group'], values='value').reset_index()
        coverage_data.columns.name = None
        return coverage_data

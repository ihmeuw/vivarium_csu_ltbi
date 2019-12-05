import pandas as pd


# noinspection PyAttributeOutsideInit
class LTBITreatmentCoverage:

    @property
    def name(self):
        return 'ltbi_treatment_coverage'

    def setup(self, builder):
        self.treatment_stream = builder.randomness.get_stream(f'{self.name}.treatment_propensity')

        self.household_tb_exposure = builder.value.get_value('household_tuberculosis.exposure')

        six_h_coverage_data = builder.data.load("six_h.coverage.proportion")
        self.six_h_with_hiv, self.six_h_under_five_hhtb = self.setup_coverage_tables(builder, six_h_coverage_data)
        three_hp_coverage_data = builder.data.load("three_hp.coverage.proportion")
        self.three_hp_with_hiv, self.three_hp_under_five_hhtb = self.setup_coverage_tables(builder,
                                                                                           three_hp_coverage_data)

        self.coverage = builder.value.register_value_producer('ltbi_treatment.coverage', source=self.get_coverage,
                                                              preferred_post_processor=self.enforce_not_eligible)

        self.columns_created = ['treatment_date', 'treatment_type']
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 requires_columns=[],
                                                 creates_columns=self.columns_created)

        self.disease_state_column = "tuberculosis_and_hiv"
        self.population_view = builder.population.get_view([self.disease_state_column, 'age', 'alive'] +
                                                           self.columns_created,
                                                           query="alive == 'alive'")

        builder.event.register_listener('time_step', self.on_time_step)

    @staticmethod
    def setup_coverage_tables(builder, coverage_data):
        with_hiv = coverage_data.loc[coverage_data.treatment_subgroup == 'with_hiv'].drop(['treatment_subgroup'], axis=1)
        under_five_hhtb = coverage_data.loc[coverage_data.treatment_subgroup == 'under_five_hhtb'].drop(['treatment_subgroup'], axis=1)
        coverage_table_with_hiv = builder.lookup.build_table(with_hiv, parameter_columns=['age', 'year'],
                                                             key_columns=['sex'],
                                                             value_columns=['value'])

        coverage_table_under_five_hhtb = builder.lookup.build_table(under_five_hhtb, parameter_columns=['age', 'year'],
                                                                    key_columns=['sex'],
                                                                    value_columns=['value'])
        return coverage_table_with_hiv, coverage_table_under_five_hhtb

    def get_coverage(self, index):
        pop = self.population_view.get(index)

        # TODO: Switch to a dataframe of zeroes with index = index, overwrite
        #  with the below
        coverage = pd.DataFrame(data={'3HP': 0.0, '6H': 0.0, 'untreated': 1.0},
                                index=pop.index)

        # Generate bit masks for our group conditions
        with_hiv = self.get_hiv_positive_subgroup(pop)
        under_five_hhtb = self.get_under_five_hhtb_subgroup(pop)

        # HIV+ Group
        with_hiv_idx = pop.loc[with_hiv].index
        with_hiv_threehp = self.three_hp_with_hiv(with_hiv_idx)
        with_hiv_sixh = self.six_h_with_hiv(with_hiv_idx)

        coverage.loc[with_hiv_idx, '3HP'] = with_hiv_threehp
        coverage.loc[with_hiv_idx, '6H'] = with_hiv_sixh
        coverage.loc[with_hiv_idx, 'untreated'] = 1. - (with_hiv_threehp + with_hiv_sixh)

        # Under 5 and exposed to household TB group
        under_five_hhtb_idx = pop.loc[under_five_hhtb].index
        under_five_hhtb_threehp = self.three_hp_under_five_hhtb(under_five_hhtb_idx)
        under_five_hhtb_sixh = self.six_h_under_five_hhtb(under_five_hhtb_idx)

        coverage.loc[under_five_hhtb_idx, '3HP'] = under_five_hhtb_threehp
        coverage.loc[under_five_hhtb_idx, '6H'] = under_five_hhtb_sixh
        coverage.loc[under_five_hhtb_idx, 'untreated'] = 1. - (under_five_hhtb_threehp + under_five_hhtb_sixh)

        # In both groups
        in_both_groups = with_hiv & under_five_hhtb
        coverage.loc[in_both_groups, '3HP'] = 1. - ((1. - with_hiv_threehp.loc[in_both_groups]) * (1. - under_five_hhtb_threehp.loc[in_both_groups]))
        coverage.loc[in_both_groups, '6H'] = 1. - ((1. - with_hiv_sixh.loc[in_both_groups]) * (1. - under_five_hhtb_sixh.loc[in_both_groups]))
        coverage.loc[in_both_groups, 'untreated'] = 1 - (coverage.loc[in_both_groups, '3HP'] + coverage.loc[in_both_groups, '6H'])

        return coverage

    def enforce_not_eligible(self, data, timestep):
        pop = self.population_view.get(data.index)

        with_hiv = self.get_hiv_positive_subgroup(pop)
        under_five_hhtb = self.get_under_five_hhtb_subgroup(pop)

        data.loc[~with_hiv & ~under_five_hhtb, '3HP'] = 0.0
        data.loc[~with_hiv & ~under_five_hhtb, '6H'] = 0.0
        data.loc[~with_hiv & ~under_five_hhtb, 'untreated'] = 1.0

        return data

    def on_initialize_simulants(self, pop_data):
        initialized = pd.DataFrame({'treatment_date': pd.NaT,
                                    'treatment_type': 'untreated'}, index=pop_data.index)
        self.population_view.update(initialized)

    def on_time_step(self, event):
        pop = self.population_view.get(event.index)

        coverage = self.coverage(event.index)

        treatment_choices = self.treatment_stream.choice(pop.index, coverage.columns, coverage)
        pop['treatment_type'] = treatment_choices
        pop.loc[treatment_choices != 'untreated', 'treatment_date'] = event.time

        self.population_view.update(pop)

    def get_hiv_positive_subgroup(self, pop):
        """Returns a bit mask of simulants in the treatment subgroup that is
        HIV+."""
        with_hiv = pop[self.disease_state_column].str.contains('positive_hiv')
        no_active_tb = self.get_not_active_tb(pop)
        not_previously_treated = self.get_not_previously_treated(pop)
        return with_hiv & no_active_tb & not_previously_treated

    def get_under_five_hhtb_subgroup(self, pop):
        """Returns a bit mask of simulants in the treatment subgroup that is
        under 5 and exposed to HHTB."""
        age_five_and_under = pop['age'] <= 5.0
        exposed_hhtb = self.household_tb_exposure(pop.index) == 'cat1'
        no_active_tb = self.get_not_active_tb(pop)
        not_previously_treated = self.get_not_previously_treated(pop)
        return age_five_and_under & exposed_hhtb & no_active_tb & not_previously_treated

    def get_not_active_tb(self, pop):
        """Returns a bit mask of simulants that do not have active TB."""
        return ~pop[self.disease_state_column].str.contains('activetb')

    @staticmethod
    def get_not_previously_treated(pop):
        return pd.isnull(pop['treatment_date'])

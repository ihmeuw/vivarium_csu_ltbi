import pandas as pd


# noinspection PyAttributeOutsideInit
class BaselineCoverage:

    @property
    def name(self):
        return 'treatment_coverage'

    def setup(self, builder):
        self.treatment_stream = builder.randomness.get_stream('treatment_propensity')

        self.household_tb_exposure = builder.value.get_value('household_tuberculosis.exposure')

        # Setup pipelines for 6H coverage for the two treatment subgroups
        six_h_coverage_data = builder.data.load("six_h.coverage.proportion")
        six_h_with_hiv, six_h_under_five_hhtb = self.setup_coverage_tables(builder, six_h_coverage_data)
        self.six_h_coverage_with_hiv = builder.value.register_value_producer('six_h_with_hiv',
                                                                             source=six_h_with_hiv)
        self.six_h_coverage_under_five_hhtb = builder.value.register_value_producer('six_h_under_five_hhtb',
                                                                                    source=six_h_under_five_hhtb)

        # Setup pipelines for 3HP coverage for the two treatment groups
        three_hp_coverage_data = builder.data.load("three_hp.coverage.proportion")
        three_hp_with_hiv, three_hp_under_five_hhtb = self.setup_coverage_tables(builder, three_hp_coverage_data)
        self.three_hp_coverage_with_hiv = builder.value.register_value_producer('three_hp_with_hiv',
                                                                                source=three_hp_with_hiv)
        self.three_hp_coverage_under_five_hhtb = builder.value.register_value_producer('three_hp_under_five_hhtb',
                                                                                       source=three_hp_under_five_hhtb)

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
        coverage_table_with_hiv = builder.lookup.build_table(with_hiv,
                                                             parameter_columns=['age', 'year'],
                                                             key_columns=['sex'],
                                                             value_columns=['value'])

        coverage_table_under_five_hhtb = builder.lookup.build_table(under_five_hhtb,
                                                                    parameter_columns=['age', 'year'],
                                                                    key_columns=['sex'],
                                                                    value_columns=['value'])
        return coverage_table_with_hiv, coverage_table_under_five_hhtb

    def on_initialize_simulants(self, pop_data):
        initialized = pd.DataFrame({'treatment_date': pd.NaT,
                                    'treatment_type': 'untreated'}, index=pop_data.index)
        self.population_view.update(initialized)

    def on_time_step(self, event):
        pop = self.population_view.get(event.index)

        # Generate bit masks for our group conditions
        with_hiv = self.get_hive_positive_subgroup(pop)
        under_five_hhtb = self.get_under_five_hhtb_subgroup(pop)

        # HIV+ Group
        with_hiv_idx = pop.loc[with_hiv].index
        with_hiv_threehp = self.three_hp_coverage_with_hiv(with_hiv_idx)
        with_hiv_sixh = self.six_h_coverage_with_hiv(with_hiv_idx)
        with_hiv_treatment_choices = self.treatment_stream.choice(with_hiv_idx, ['3HP', '6H', 'untreated'],
                                                                  pd.concat([with_hiv_threehp, with_hiv_sixh,
                                                                             (1 - with_hiv_threehp + with_hiv_sixh)],
                                                                            axis=1))
        with_hiv_treated = with_hiv_treatment_choices.loc[with_hiv_treatment_choices != 'untreated']
        pop.loc[with_hiv_treated.index, 'treatment_date'] = event.time
        pop.loc[with_hiv_treated.index, 'treatment_type'] = with_hiv_treated

        # Under 5 and exposed to household TB group
        under_five_hhtb_idx = pop.loc[under_five_hhtb].index
        under_five_hhtb_threehp = self.three_hp_coverage_under_five_hhtb(under_five_hhtb_idx)
        under_five_hhtb_sixh = self.six_h_coverage_under_five_hhtb(under_five_hhtb_idx)
        under_five_hhtb_treatment_choices = self.treatment_stream.choice(under_five_hhtb_idx, ['3HP', '6H', 'untreated'],
                                                                         pd.concat([under_five_hhtb_threehp,
                                                                                    under_five_hhtb_sixh,
                                                                                    (1 - under_five_hhtb_threehp + under_five_hhtb_sixh)],
                                                                                   axis=1))
        under_five_hhtb_treated = under_five_hhtb_treatment_choices.loc[under_five_hhtb_treatment_choices != 'untreated']
        pop.loc[under_five_hhtb_treated.index, 'treatment_date'] = event.time
        pop.loc[under_five_hhtb_treated.index, 'treatment_type'] = under_five_hhtb_treated

        self.population_view.update(pop)

    def get_hive_positive_subgroup(self, pop):
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

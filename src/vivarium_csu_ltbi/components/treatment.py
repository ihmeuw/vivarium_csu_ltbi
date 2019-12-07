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

        import pdb
        pdb.set_trace()

        coverage_data = builder.data.load("risk_factor.ltbi_treatment.coverage")
        _coverage_raw = builder.lookup.build_table(coverage_data, parameter_columns=['age', 'year'],
                                                             key_columns=['sex', 'treatment_subgroup',
                                                                          'treatment_type'],
                                                             value_columns=['value'])

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

        self.columns_created = ['treatment_date', 'treatment_type', 'adherence_propensity']
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
                                    'adherence_propensity': self.adherence_stream.get_draw(pop_data.index)},
                                   index=pop_data.index)
        self.population_view.update(initialized)

    def on_time_step_prepare(self, event):
        pop = self.population_view.get(event.index, query="treatment_type == 'untreated'")

        coverage = self.coverage_filtered(event.index)

        treatment_type = self.treatment_stream.choice(pop.index, coverage.columns, coverage)
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

        import pdb
        pdb.set_trace()

        raw_coverage = self.coverage_raw(pop.index)
        # raw_coverage.loc [raw_coverage.treatment_type=='3HP']



        df_with_hiv = raw_coverage.loc[(with_hiv) & (raw_coverage['treatment_subgroup']=='with_hiv')]
        coverage.loc[with_hiv, '3HP'] = df_with_hiv.loc[df_with_hiv.treatment_type=='3HP', 'value']
        coverage.loc[with_hiv, '6H'] = df_with_hiv.loc[df_with_hiv.treatment_type=='6H', 'value']

        df_under5_hhtb = raw_coverage.loc[(under_five_hhtb) & (raw_coverage['treatment_subgroup']=='under_five_hhtb')]
        coverage.loc[under_five_hhtb, '3HP'] = df_with_hiv.loc[df_under5_hhtb.treatment_type=='3HP', 'value']
        coverage.loc[under_five_hhtb, '6H'] = df_with_hiv.loc[df_under5_hhtb.treatment_type=='6H', 'value']

        in_both_groups = with_hiv & under_five_hhtb
        df_with_both_groups = coverage[in_both_groups]
        # coverage.loc[in_both_groups, '3HP'] = 1. - (
        #         (1. -  df_with_hiv.loc[df_with_hiv.treatment_type=='3HP', 'value'])
        #                                             * (1. - under_five_hhtb_threehp.loc[in_both_groups]))
        #




        # # HIV+ Group
        # with_hiv_idx = pop.loc[with_hiv].index
        # with_hiv_threehp = self.three_hp_with_hiv(with_hiv_idx)
        # with_hiv_sixh = self.six_h_with_hiv(with_hiv_idx)
        #
        # coverage.loc[with_hiv_idx, '3HP'] = with_hiv_threehp
        # coverage.loc[with_hiv_idx, '6H'] = with_hiv_sixh
        # coverage.loc[with_hiv_idx, 'untreated'] = 1. - (with_hiv_threehp + with_hiv_sixh)
        #
        # # Under 5 and exposed to household TB group
        # under_five_hhtb_idx = pop.loc[under_five_hhtb].index
        # under_five_hhtb_threehp = self.three_hp_under_five_hhtb(under_five_hhtb_idx)
        # under_five_hhtb_sixh = self.six_h_under_five_hhtb(under_five_hhtb_idx)
        #
        # coverage.loc[under_five_hhtb_idx, '3HP'] = under_five_hhtb_threehp
        # coverage.loc[under_five_hhtb_idx, '6H'] = under_five_hhtb_sixh
        # coverage.loc[under_five_hhtb_idx, 'untreated'] = 1. - (under_five_hhtb_threehp + under_five_hhtb_sixh)
        #
        # # In both groups
        # in_both_groups = with_hiv & under_five_hhtb
        # coverage.loc[in_both_groups, '3HP'] = 1. - ((1. - with_hiv_threehp.loc[in_both_groups])
        #                                             * (1. - under_five_hhtb_threehp.loc[in_both_groups]))
        # coverage.loc[in_both_groups, '6H'] = 1. - ((1. - with_hiv_sixh.loc[in_both_groups])
        #                                            * (1. - under_five_hhtb_sixh.loc[in_both_groups]))
        # coverage.loc[in_both_groups, 'untreated'] = 1 - (coverage.loc[in_both_groups, '3HP']
        #                                                  + coverage.loc[in_both_groups, '6H'])

        return coverage

    def enforce_not_eligible(self, data, timestep):
        pop = self.population_view.subview(['age', ltbi_globals.TUBERCULOSIS_AND_HIV]).get(data.index)

        with_hiv = self.get_hiv_positive_subgroup(pop)
        under_five_hhtb = self.get_under_five_hhtb_subgroup(pop)

        data.loc[~with_hiv & ~under_five_hhtb, '3HP'] = 0.0
        data.loc[~with_hiv & ~under_five_hhtb, '6H'] = 0.0
        data.loc[~with_hiv & ~under_five_hhtb, 'untreated'] = 1.0

        return data

    def get_hiv_positive_subgroup(self, pop):
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

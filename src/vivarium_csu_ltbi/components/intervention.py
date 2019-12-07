

class LTBITreatmentScaleUp:

    configuration_defaults = {
        'ltbi_treatment_scale_up': {
            'scenario': 'baseline'
        }
    }

    @property
    def name(self):
        return 'ltbi_treatment_scale_up'

    def setup(self, builder):
        coverage_shift_data = self.load_coverage_shift_data(builder)
        self.coverage_shift = builder.lookup.build_table(coverage_shift_data,
                                                         parameter_columns=['age', 'year'],
                                                         key_columns=['sex'],
                                                         value_columns=['with_hiv_6H', 'with_hiv_3HP',
                                                                        'under_five_hhtb_6H', 'under_five_hhtb_3HP'])
        builder.value.register_value_modifier('ltbi_treatment.data', self.adjust_coverage,
                                              requires_columns=['age', 'sex'])

    def adjust_coverage(self, index, coverage):
        updated_coverage = coverage + self.coverage_shift(index)
        return updated_coverage

    @staticmethod
    def load_coverage_shift_data(builder):
        scenario = builder.configuration.ltbi_treatment_scale_up.scenario
        shift_data = builder.data.load('ltbi_treatment.intervention_coverage_shift')
        shift_data = shift_data.loc[shift_data.scenario == scenario].drop(columns=['scenario'])
        shift_data['treatment_group'] = shift_data['treatment_subgroup'] + '_' + shift_data['treatment_type']
        shift_data = shift_data.drop(['treatment_subgroup', 'treatment_type'], axis=1)
        key_cols = ['sex', 'age_start', 'age_end', 'year_start', 'year_end']
        shift_data = shift_data.pivot_table(index=key_cols, columns=['treatment_group'],
                                            values='value').reset_index()
        shift_data.columns.name = None
        return shift_data


from vivarium_public_health.utilities import EntityString, TargetString
from vivarium_public_health.risks.data_transformations import (get_relative_risk_data,
                                                               get_population_attributable_fraction_data)


class HHTBCorrelatedRiskEffect:
    """A custom risk effect that targets prevalence and birth
    prevalence of LTBI and adjusts it to correlate it with household
    tuberculosis"""

    configuration_defaults = {
        'effect_of_risk_on_target': {
            'measure': {}
        }
    }

    def __init__(self, target):  # sequela.ltbi_susceptible_hiv, sequela.ltbi_positive_hiv
        self.risk = EntityString('risk_factor.household_tuberculosis')
        self.target = TargetString(target)
        self.configuration_defaults = {
            f'effect_of_{self.risk.name}_on_{self.target.name}': {
                self.target.measure:
                    HHTBCorrelatedRiskEffect.configuration_defaults['effect_of_risk_on_target']['measure']
            }
        }

    @property
    def name(self):
        return f'risk_effect.household_tuberculosis.{self.target.name}.{self.target.measure}'

    def setup(self, builder):
        self.randomness = builder.randomness.get_stream(
            f'effect_of_{self.risk.name}_on_{self.target.name}.{self.target.measure}'
        )

        self.relative_risk = self._get_relative_risk_date(builder)
        self.exposed_exposure_probability = self._get_exposed_exposure_probability_data(builder)
        self.exposure = builder.value.get_value(f'{self.risk.name}.exposure')
        self.population_attributable_fraction = self._get_population_attributable_fraction_data(builder)

        builder.value.register_value_modifier(f'{self.target.name}.{self.target.measure}',
                                              modifier=self.adjust_target,
                                              requires_values=[f'{self.risk.name}.exposure'],
                                              requires_columns=['age', 'sex'])

    def _get_relative_risk_date(self, builder):
        relative_risk_data = get_relative_risk_data(builder, self.risk, self.target, self.randomness)
        relative_risk = builder.lookup.build_table(relative_risk_data, key_columns=['sex'],
                                                   parameter_columns=['age', 'year'])
        return relative_risk

    def _get_exposed_exposure_probability_data(self, builder):
        exposure_data = builder.data.load(f'risk_factor.{self.risk.name}.exposure')
        exposure_data = exposure_data.loc[exposure_data['parameter'] == 'cat1']
        exposure_data = exposure_data.drop(['parameter'], axis=1)
        exposure = builder.lookup.build_table(exposure_data, key_columns=['sex'],
                                              parameter_columns=['age', 'year'])
        return exposure

    def _get_population_attributable_fraction_data(self, builder):
        paf_data = get_population_attributable_fraction_data(builder, self.risk, self.target, self.randomness)
        paf = builder.lookup.build_table(paf_data, key_columns=['sex'],
                                         parameter_columns=['age', 'year'])
        return paf

    def adjust_target(self, index, target):
        is_exposed = self.exposure(index) == 'cat1'
        target.loc[is_exposed] *= ((1. - self.population_attributable_fraction(index)) *
                                   self.relative_risk(index)['cat1'] *
                                   self.exposed_exposure_probability(index))
        target.loc[~is_exposed] *= ((1. - self.population_attributable_fraction(index)) *
                                    (1. - self.exposed_exposure_probability(index)))
        return target

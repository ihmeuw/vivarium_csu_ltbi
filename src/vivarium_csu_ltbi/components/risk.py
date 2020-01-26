from vivarium_public_health.utilities import EntityString, TargetString
from vivarium_public_health.risks.data_transformations import (get_relative_risk_data,
                                                               get_population_attributable_fraction_data)

from vivarium_csu_ltbi import globals as ltbi_globals


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
        self.risk = EntityString(f'risk_factor.{ltbi_globals.HOUSEHOLD_TUBERCULOSIS}')
        self.target = TargetString(target)
        self.configuration_defaults = {
            f'effect_of_{self.risk.name}_on_{self.target.name}': {
                self.target.measure:
                    HHTBCorrelatedRiskEffect.configuration_defaults['effect_of_risk_on_target']['measure']
            }
        }

    @property
    def name(self):
        return f'risk_effect.{ltbi_globals.HOUSEHOLD_TUBERCULOSIS}.{self.target.name}.{self.target.measure}'

    def setup(self, builder):
        self.randomness = builder.randomness.get_stream(
            f'effect_of_{self.risk.name}_on_{self.target.name}.{self.target.measure}'
        )

        self.relative_risk = self._get_relative_risk_data(builder)
        self.exposure = builder.value.get_value(f'{self.risk.name}.exposure')
        self.population_attributable_fraction = self._get_population_attributable_fraction_data(builder)

        builder.value.register_value_modifier(f'{self.target.name}.{self.target.measure}',
                                              modifier=self.adjust_target,
                                              requires_values=[f'{self.risk.name}.exposure'],
                                              requires_columns=['age', 'sex'])

    def _get_relative_risk_data(self, builder):
        relative_risk_data = get_relative_risk_data(builder, self.risk, self.target, self.randomness)
        relative_risk = builder.lookup.build_table(relative_risk_data, key_columns=['sex'],
                                                   parameter_columns=['age', 'year'])
        return relative_risk

    def _get_population_attributable_fraction_data(self, builder):
        paf_data = get_population_attributable_fraction_data(builder, self.risk, self.target, self.randomness)
        paf = builder.lookup.build_table(paf_data, key_columns=['sex'],
                                         parameter_columns=['age', 'year'])
        return paf

    def adjust_target(self, index, target):
        exposure = self.exposure(index)
        paf = self.population_attributable_fraction(index)
        rr = self.relative_risk(index).lookup(index, exposure)
        target *= (1. - paf) * rr
        target.loc[target > 1.0] = 1.0
        return target

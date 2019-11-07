import pandas as pd

from gbd_mapping import causes

from vivarium_inputs.interface import get_measure
from vivarium_inputs.data_artifact.utilities import split_interval


def load_data(country_name: str):
    """output LTBI prevalence, LTBI excess mortality, and All Causes cause-specific mortality"""
    p_ltbi = get_measure(causes.latent_tuberculosis_infection, 'prevalence', country_name)

    i_actb = pd.DataFrame(0.0, index=p_ltbi.index, columns=['draw_' + str(i) for i in range(1000)])

    # aggregate all child active TB causes incidence to obtain all-form active TB incidence
    active_tb_names = ['drug_susceptible_tuberculosis',
                       'multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
                       'extensively_drug_resistant_tuberculosis',
                       'hiv_aids_drug_susceptible_tuberculosis',
                       'hiv_aids_multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
                       'hiv_aids_extensively_drug_resistant_tuberculosis']
    for actb in active_tb_names:
        i_actb += get_measure(getattr(causes, actb), 'incidence', country_name)

    f_ltbi = i_actb / p_ltbi

    csmr_all = get_measure(causes.all_causes, 'cause_specific_mortality', country_name)

    p_ltbi = split_interval(p_ltbi, interval_column='age', split_column_prefix='age_group')
    p_ltbi = split_interval(p_ltbi, interval_column='year', split_column_prefix='year')
    f_ltbi = split_interval(f_ltbi, interval_column='age', split_column_prefix='age_group')
    f_ltbi = split_interval(f_ltbi, interval_column='year', split_column_prefix='year')
    csmr_all = split_interval(csmr_all, interval_column='age', split_column_prefix='age_group')
    csmr_all = split_interval(csmr_all, interval_column='year', split_column_prefix='year')

    return p_ltbi, f_ltbi, csmr_all

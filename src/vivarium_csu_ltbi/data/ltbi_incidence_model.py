import pandas as pd
import numpy as np

from gbd_mapping import causes

from vivarium_inputs.interface import get_measure
from vivarium_inputs.data_artifact.utilities import split_interval

from vivarium_csu_ltbi import globals as ltbi_globals


def load_data(country_name: str):
    """output LTBI prevalence, LTBI excess mortality, and All Causes cause-specific mortality"""
    p_ltbi = get_measure(causes.latent_tuberculosis_infection, 'prevalence', country_name)

    i_actb = pd.DataFrame(0.0, index=p_ltbi.index, columns=['draw_' + str(i) for i in range(1000)])

    # aggregate all child active TB causes incidence to obtain all-form active TB incidence
    for actb in ltbi_globals.GBD_ACTIVE_TB_NAMES:
        i_actb += get_measure(getattr(causes, actb), 'incidence_rate', country_name)

    f_ltbi = i_actb / p_ltbi

    csmr_all = get_measure(causes.all_causes, 'cause_specific_mortality_rate', country_name)

    p_ltbi = split_interval(p_ltbi, interval_column='age', split_column_prefix='age_group')
    p_ltbi = split_interval(p_ltbi, interval_column='year', split_column_prefix='year')
    f_ltbi = split_interval(f_ltbi, interval_column='age', split_column_prefix='age_group')
    f_ltbi = split_interval(f_ltbi, interval_column='year', split_column_prefix='year')
    csmr_all = split_interval(csmr_all, interval_column='age', split_column_prefix='age_group')
    csmr_all = split_interval(csmr_all, interval_column='year', split_column_prefix='year')

    return p_ltbi, f_ltbi, csmr_all


def format_for_dismod(df: pd.DataFrame, draw: int, sex: str, year: int, data_type: str):
    """prepare data into dismod format"""
    df = df.stack().reset_index().rename(columns={'level_6': 'draw', 0: 'value'})
    df['draw'] = df.draw.map(lambda x: int(x.split('_')[1]))

    df_sub = df.query(f'draw == {draw} and sex == "{sex}" and year_start == {year}').reset_index(drop=True)
    df_sub['data_type'] = data_type
    df_sub['area'] = 'all'
    df_sub['sex'] = 'total'
    df_sub['standard_error'] = 0.01
    df_sub['upper_ci'] = np.nan
    df_sub['lower_ci'] = np.nan
    df_sub['effective_sample_size'] = 1_000

    df_sub = df_sub.rename(columns={'age_group_start': 'age_start', 'age_group_end': 'age_end'})
    return df_sub


def make_disease_model(p: pd.DataFrame, f: pd.DataFrame, m_all: pd.DataFrame, knots: list):
    import dismod_mr
    dm = dismod_mr.data.ModelData()
    # prepare dismod input data
    dm.input_data = pd.concat([p, f, m_all], ignore_index=True)
    # set age knots
    for rate_type in 'ifr':
        dm.set_knots(rate_type, knots)
    # set proper value and boundaries for dismod input measures
    dm.set_level_bounds('i', lower=1e-3, upper=1e-1)
    dm.set_level_bounds('f', lower=1e-4, upper=1e-1)
    dm.set_level_value('r', value=0, age_before=101, age_after=101)
    dm.set_level_bounds('r', lower=1e-6, upper=1)
    dm.set_level_value('p', value=.2, age_before=1, age_after=101)

    return dm


def fit_and_predict(p: pd.DataFrame, f: pd.DataFrame, m_all: pd.DataFrame, knots: list):
    """predict LTBI incidence for certain country, sex, and year
    based on single draw input data"""
    import pymc as pm
    dm = make_disease_model(p, f, m_all, knots)
    dm.setup_model(rate_model='normal', include_covariates=False)
    # set all dismod variables to maximum a posteriori values
    m = pm.MAP(dm.vars)
    m.fit()

    i_ltbi = dm.vars['i']['mu_age'].value
    return i_ltbi


def format_for_art(i_ltbi: np.ndarray, draw: int, country_name: str, sex: str, year: int):
    """prepare LTBI incidence back calculated from dismod into sim data artifact format"""
    df = pd.DataFrame({'value': i_ltbi[:-1]})
    df['draw'] = draw
    df['location'] = country_name
    df['age_group_start'] = np.arange(0, 100)
    df['age_group_end'] = np.arange(1, 101)
    df['sex'] = sex
    df['year_start'] = year
    df['year_end'] = year + 1
    df['parameter'] = 'continuous'

    return df

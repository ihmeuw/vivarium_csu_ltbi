import pandas as pd
import numpy as np

from gbd_mapping import causes
from vivarium.interpolation import Interpolation
from vivarium_inputs.interface import get_measure
from vivarium_inputs.data_artifact.utilities import split_interval

from vivarium_csu_ltbi import globals as ltbi_globals
from vivarium_csu_ltbi import paths as ltbi_paths


def load_household_input_data(location: str):
    input_data_path = ltbi_paths.get_hh_tb_input_data_path(location)

    df = pd.read_stata(input_data_path).dropna()

    formatted_location = ltbi_globals.formatted_location(location)
    if formatted_location == 'south_africa':
        df['age'] = df['age'].replace({'Less than 1 year': '0',
                                       'less than 1 year': '0',
                                       '1 year': '1',
                                       '2 years': '2',
                                       '100+': '100'})
    else:

        df['hh_id'] = df['hh_id'].str.split().map(lambda x: int(''.join(x)))
        df['sex'] = df['sex'].str.capitalize()
        df['age'] = df['age'].replace({'95+': 95, '96+': 95})
        df = df[~((df.age == "don't know") | (df.age == 'dk'))]
    df['age'] = df['age'].astype(float)
    return df


def load_actb_prevalence_input_data(location: str):
    """output all-form TB prevalence."""
    prev_ltbi = get_measure(causes.latent_tuberculosis_infection, 'prevalence', location)
    prev_actb = pd.DataFrame(0.0, index=prev_ltbi.index, columns=['draw_' + str(i) for i in range(1000)])

    # aggregate all child active TB causes prevalence to obtain all-form TB prevalence
    for actb in ltbi_globals.GBD_ACTIVE_TB_NAMES:
        prev_actb += get_measure(getattr(causes, actb), 'prevalence', location)

    index_cols = ['location', 'sex', 'age_group_start', 'year_start', 'age_group_end', 'year_end', 'draw']
    prev_actb = split_interval(prev_actb, interval_column='age', split_column_prefix='age_group')
    prev_actb = split_interval(prev_actb, interval_column='year', split_column_prefix='year')
    prev_actb = prev_actb.reset_index().melt(id_vars=index_cols[:-1], var_name=['draw'])
    prev_actb['draw'] = prev_actb.draw.map(lambda x: int(x.split('_')[1]))

    return prev_actb


def interpolation(prev_actb: pd.DataFrame, df: pd.DataFrame, year_start: int, draw: int):
    """assign the probability of active TB for each simulant."""

    interp = Interpolation(prev_actb.query(f'year_start == {year_start} and draw == {draw}'),
                           categorical_parameters=['sex'],
                           continuous_parameters=[['age', 'age_group_start', 'age_group_end']],
                           order=0,
                           extrapolate=True)

    df['pr_actb'] = interp(df).value
    return df


def calc_pr_actb_in_hh(df: pd.DataFrame):
    """compute the probability that there is at least one person with active TB
    for each household."""

    pr_no_tb = 1 - df.pr_actb
    pr_no_tb_in_hh = np.prod(pr_no_tb)
    return 1 - pr_no_tb_in_hh


def age_sex_specific_actb_prop(df: pd.DataFrame):
    """calculate the probability of an active TB case in the household
    for certain age and sex group."""

    df = df.set_index('hh_id')
    age_bins = [0, 1] + list(range(5, 96, 5)) + [125]
    res = pd.DataFrame()
    for i, age_group_start in enumerate(age_bins[:-1]):
        age_group_end = age_bins[i + 1]
        for sex in ['Male', 'Female']:
            hh_with = df.query(f'age >= {age_group_start} and age < {age_group_end} and sex == "{sex}"').index.unique()
            prop = df.loc[hh_with].groupby(level=0).apply(calc_pr_actb_in_hh)
            prop_mean = prop.mean()
            age_sex_res = pd.DataFrame({'age_group_start': [age_group_start], 'age_group_end': [age_group_end],
                                        'sex': [sex], 'pr_actb_in_hh': [prop_mean]})
            res = res.append(age_sex_res, ignore_index=True)
    return res


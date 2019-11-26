import pandas as pd
import numpy as np
from gbd_mapping import causes
from vivarium.interpolation import Interpolation
from vivarium_inputs.interface import get_measure
from vivarium_inputs.data_artifact.utilities import split_interval

master_dir = '/home/j/Project/simulation_science/latent_tuberculosis_infection/literature/household_structure/microdata/'
index_cols = ['location', 'sex', 'age_group_start', 'year_start', 'age_group_end', 'year_end', 'draw']
actb_names = ['drug_susceptible_tuberculosis', 
              'multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
              'extensively_drug_resistant_tuberculosis',
              'hiv_aids_drug_susceptible_tuberculosis',
              'hiv_aids_multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
              'hiv_aids_extensively_drug_resistant_tuberculosis']

def load_hh_data(country_name: str):
    "format household microdata"
    if country_name == 'South Africa':
        country_name = 'South_Africa'
    
    df = pd.read_stata(master_dir + country_name + '.dta').dropna()
    
    if country_name == 'South_Africa':
        df['age'] = df['age'].replace({'less than 1 year': '0',
                                       '1 year': '1',
                                       '2 years': '2',
                                       '100+': '100'}).astype(float)
    else:
        df['hh_id'] = df['hh_id'].str.split().map(lambda x: int(''.join(x)))
        df['sex'] = df['sex'].str.capitalize()
        df['age'] = df['age'].replace({'95+': 95, '96+': 96})
        df = df[~((df.age == "don't know") | (df.age == "dk"))]
        df['age'] = df['age'].astype(float)
    
    return df

def load_and_transform(country_name: str):
    """output all-form TB prevalence"""
    prev_ltbi = get_measure(causes.latent_tuberculosis_infection, 'prevalence', country_name)
    
    prev_actb = pd.DataFrame(0.0, index=prev_ltbi.index, columns=['draw_' + str(i) for i in range(1000)])
    # aggregate all child active TB causes prevalence to obtain all-form TB prevalence
    for actb in actb_names:
        prev_actb += get_measure(getattr(causes, actb), 'prevalence', country_name)

    prev_actb = split_interval(prev_actb, interval_column='age', split_column_prefix='age_group')
    prev_actb = split_interval(prev_actb, interval_column='year', split_column_prefix='year')
    prev_actb = prev_actb.reset_index().melt(id_vars=index_cols[:-1], var_name=['draw'])
    prev_actb['draw'] = prev_actb.draw.map(lambda x: int(x.split('_')[1]))
    
    return prev_actb

def interpolation(prev_actb: pd.DataFrame, df: pd.DataFrame, year_start: int, draw: int):
    """assign the probability of active TB for each simulant"""
    interp = Interpolation(prev_actb.query(f'year_start == {year_start} and draw == {draw}'),
                           categorical_parameters=['sex'],
                           continuous_parameters=[['age', 'age_group_start', 'age_group_end']],
                           order=0,
                           extrapolate=True)

    df['pr_actb'] = interp(df).value
    return df

def calc_pr_actb_in_hh(df: pd.DataFrame):
    """compute the probability that 
    there is at least one person with active TB for each household
    """
    pr_no_tb = 1 - df.pr_actb
    pr_no_tb_in_hh = np.prod(pr_no_tb)
    return 1 - pr_no_tb_in_hh

def age_sex_specific_actb_prop(df: pd.DataFrame):
    """calculate the probability of an active TB case in the household
    for certain age and sex group
    """
    age_bins = [0, 1] + list(range(5, 96, 5)) + [125]
    res = pd.DataFrame()
    for i, age_group_start in enumerate(age_bins[:-1]):
        age_group_end = age_bins[i+1]
        for sex in ['Male', 'Female']:
            hh_with = df.query(f'age >= {age_group_start} and age < {age_group_end} and sex == "{sex}"').hh_id.unique()
            prop = df[df.hh_id.isin(hh_with)].groupby('hh_id').apply(calc_pr_actb_in_hh)
            prop_mean = prop.mean()
            age_sex_res = pd.DataFrame({'age_group_start': [age_group_start], 'age_group_end': [age_group_end], 
                                        'sex': [sex], 'pr_actb_in_hh': [prop_mean]})
            res = res.append(age_sex_res, ignore_index=True)
    return res

def country_specific_outputs(country_name: str, year_start=2017):
    """for certain country in year 2017,
    sweep over draw
    """
    outputs = pd.DataFrame()
    df_hh = load_hh_data(country_name)
    hh_ids = df_hh.hh_id.unique()
    prev_actb = load_and_transform(country_name)

    for draw in range(1000):
        # bootstrap HH data by resampling hh_id with replacement
        sample_hhids = np.random.choice(hh_ids, size=len(hh_ids), replace=True)
        df_hh_sample = pd.DataFrame()
        for i in sample_hhids:
            df_hh_sample = df_hh_sample.append(df_hh[df_hh.hh_id == i])
        
        data = interpolation(prev_actb, df_hh_sample, year_start, draw)
        res = age_sex_specific_actb_prop(data)
        res['location'] = country_name
        res['year_start'] = year_start
        res['year_end'] = year_start + 1
        res['draw'] = draw
        outputs = pd.concat([outputs, res], ignore_index=True)
    
    outputs = outputs.set_index(index_cols)
    return outputs


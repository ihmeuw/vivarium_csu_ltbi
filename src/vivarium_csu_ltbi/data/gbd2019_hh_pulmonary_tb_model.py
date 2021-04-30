import numpy as np
import pandas as pd
from get_draws.api import get_draws
from vivarium.interpolation import Interpolation

master_dir = '/home/j/Project/simulation_science/latent_tuberculosis_infection/literature/household_structure/microdata/'
country_dict = {
    6: 'China',
    11: 'Indonesia',
    15: 'Myanmar',
    16: 'Philippines',
    20: 'Vietnam',
    62: 'Russian_Federation',
    161: 'Bangladesh',
    163: 'India',
    165: 'Pakistan',
    168: 'Angola',
    171: 'Congo',
    179: 'Ethiopia',
    180: 'Kenya',
    184: 'Mozambique',
    189: 'Tanzania',
    190: 'Uganda',
    191: 'Zambia',
    196: 'South_Africa',
    198: 'Zimbabwe',
    214: 'Nigeria'
}
age_group_ids = [1, 23, 149, 150, 151, 152, 153, 154]
age_group_names = ['0_to_5', '5_to_15'] + [f'{c}_to_{c+10}' for c in range(15, 65, 10)] + ['65_to_125'] 
age_dict = dict(zip(age_group_ids, age_group_names))
sex_dict = {1: 'Male', 2: 'Female'}

def load_hh_data(location_id: int):
    country_name = country_dict[location_id]
    df = pd.read_stata(master_dir + country_name + '.dta').dropna()
    if 'hhid' in df.columns:
        df.rename(columns={'hhid': 'hh_id'}, inplace=True)
    if country_name in ['China', 'Russian_Federation', 'South_Africa']:
        df['age'] = df['age'].replace({'Less than 1 year': '0',
                                       'less than 1 year': '0',
                                       '1 year': '1',
                                       '2 years': '2',
                                       '100+': '100'})
        df = df[df.age != "not reported/missing"]
    else:
        df['hh_id'] = df['hh_id'].str.split().map(lambda x: int(''.join(x)))
        if country_name != 'Congo':
            df['age'] = df['age'].replace({'95+': 95, '96+': 95, '97+': 95})
            df = df[~((df.age == "don't know") | (df.age == "dk") | (df.age == "Don't know"))]
    df['sex'] = df['sex'].str.capitalize()
    df['age'] = df['age'].astype(float)
    return df

def pull_actb_prevalence(location_id: int):
    prev_actb = get_draws(
        gbd_id_type='cause_id',
        source='como',
        gbd_id=[934, 946, 947, 948, 949, 950],
        location_id=location_id,
        age_group_id=age_group_ids,
        sex_id=[1,2],
        year_id=2019,
        measure_id=5,
        metric_id=3, #ratio
        gbd_round_id=6,
        decomp_step='step5',
    )
    # get all-form TB prevalence
    prev_actb_agg = (prev_actb
                     .groupby(['location_id', 'age_group_id', 'sex_id', 'year_id', 'measure_id', 'metric_id'])
                     .sum()
                     .reset_index())
    prev_actb_agg.drop(columns=['measure_id', 'metric_id', 'cause_id'], inplace=True)
    return prev_actb_agg

def pull_extra_pulmonary_tb_frac(location_id: int):
    frac = pd.read_csv(f'/ihme/covariates/ubcov/model/output/56726/draws_temp_0/{location_id}.csv')
    frac = frac[frac.year_id == 2019].reset_index(drop=True)
    frac['age_group_id'] = frac.age_group_id.map({5:1,  6:23,  8:149, 10:150, 12:151, 14:152, 16:153, 18:154})
    return frac

def format_prevalence_data(prev: pd.DataFrame):
    prev = prev.copy()
    prev['location'] = prev.location_id.map(country_dict)
    prev['age_group_name'] = prev.age_group_id.map(age_dict)
    prev['sex'] = prev.sex_id.map(sex_dict)
    prev['year'] = 2019
    prev['age_group_start'] = prev.age_group_name.map(lambda x: int(x.split('_to_')[0]))
    prev['age_group_end'] = prev.age_group_name.map(lambda x: int(x.split('_to_')[1]))
    prev.drop(columns=['location_id', 'age_group_id', 'age_group_name', 'sex_id', 'year_id'], inplace=True)
    prev_long = prev.melt(id_vars=['location', 'age_group_start', 'age_group_end', 'sex', 'year'], var_name=['draw'])
    prev_long['draw'] = prev_long.draw.map(lambda x: int(x.split('_')[1]))
    return prev_long

def get_pulmonary_tb_prev(prev_actb: pd.DataFrame, frac: pd.DataFrame):
    prev = format_prevalence_data(prev_actb).set_index(['location', 'age_group_start', 'age_group_end', 'sex', 'year', 'draw']) 
    prop = format_prevalence_data(frac).set_index(['location', 'age_group_start', 'age_group_end', 'sex', 'year', 'draw'])
    prev_pulmonary_tb = prev * (1 - prop)
    return prev_pulmonary_tb.reset_index()

def interpolation(prev_pulmonary_tb: pd.DataFrame, hh_data: pd.DataFrame, draw: int):
    """assign the probability of active pulmonary TB for each household member"""
    mask = hh_data.copy()
    interp = Interpolation(prev_pulmonary_tb.query(f'draw == {draw}'),
                           categorical_parameters=['sex'],
                           continuous_parameters=[['age', 'age_group_start', 'age_group_end']],
                           order=0,
                           extrapolate=True,
                           validate=True)
    mask['pr_pulmonary_tb'] = interp(mask).value
    return mask

def calc_pr_no_pulmonary_tb_in_hh(df: pd.DataFrame):
    """compute the probability that no person with active pulmonary TB in a single household"""
    pr_no_tb = 1 - df.pr_pulmonary_tb
    pr_no_tb_in_hh = np.prod(pr_no_tb)
    return pr_no_tb_in_hh

def calc_age_sex_specific_no_pulmonary_tb_prop(df: pd.DataFrame):
    """
    calculate the probability of no active pulmonary TB case in the household
    for certain age and sex group
    """
    age_bins = [0, 5] + list(range(15, 70, 10)) + [125]
    result = pd.DataFrame()
    for i, age_group_start in enumerate(age_bins[:-1]):
        age_group_end = age_bins[i+1]
        for sex in ['Male', 'Female']:
            hh_with = df.query(f'age >= {age_group_start} and age < {age_group_end} and sex == "{sex}"').hh_id.unique()
            prop = df[df.hh_id.isin(hh_with)].groupby('hh_id').apply(calc_pr_no_pulmonary_tb_in_hh)
            prop_mean = prop.mean()
            age_sex_result = pd.DataFrame({'age_group_start': [age_group_start], 
                                           'age_group_end': [age_group_end], 
                                           'sex': [sex], 
                                           'value': [prop_mean]})
            result = pd.concat([result, age_sex_result], ignore_index=True)
    return result

def get_estimates(draw: int):
    """calculate the prevalence of household pulmonary TB contact"""
    output = pd.DataFrame()
    cols = ['age_group_start', 'age_group_end', 'sex']
    for location_id in country_dict.keys():
        df_hh = load_hh_data(location_id)
        hh_ids = df_hh.hh_id.unique()
        # boostrap HH data by resampling hh_id with replacement
        sample_hhids = np.random.choice(hh_ids, size=min(len(hh_ids), 50000), replace=True)
        df_hh = df_hh.set_index("hh_id")
        df_hh_sample = df_hh.loc[sample_hhids].reset_index()
        prev_actb = pull_actb_prevalence(location_id)
        frac = pull_extra_pulmonary_tb_frac(location_id) 
        prev_pulmonary_tb = get_pulmonary_tb_prev(prev_actb, frac)
        data = interpolation(prev_pulmonary_tb, df_hh_sample, draw)
        result = calc_age_sex_specific_no_pulmonary_tb_prop(data)
        prev_pulmonary_tb_draw = prev_pulmonary_tb.query(f'draw == {draw}')
        prev_pulmonary_tb_draw['value'] = 1 - prev_pulmonary_tb_draw['value']
        # prev_hhc = 1 - pr_no_pulmonary_tb_in_hh / prev_susceptible_to_pulmonary_tb
        f = (1 - result.set_index(cols).value / prev_pulmonary_tb_draw.set_index(cols).value).reset_index()
        f['location'] = country_dict[location_id]
        output = pd.concat([output, f], ignore_index=True)
    output['year_start'] = 2019
    output['year_end'] = 2020
    output['draw'] = draw
    return output.set_index(['location', 'sex', 'age_group_start', 'year_start', 'age_group_end', 'year_end', 'draw']).reset_index()


if __name__ == '__main__':
    import sys
    import os
    try:
        draw = int(os.environ['SGE_TASK_ID']) - 1
    except (KeyError, ValueError):
        draw = int(sys.argv[1])

    output = get_estimates(draw)
    output.to_hdf(f'/share/scratch/users/yongqx2/hh_pulmonary_tb_estimates/draw_{draw}.hdf', 'draw')

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

def pull_actb_incidence(location_id: int):
    incidence_actb = get_draws(
        gbd_id_type='cause_id',
        source='como',
        gbd_id=[934, 946, 947, 948, 949, 950],
        location_id=location_id,
        age_group_id=age_group_ids,
        sex_id=[1,2],
        year_id=2019,
        measure_id=6, #incidence
        metric_id=3, #ratio relative to population
        gbd_round_id=6,
        decomp_step='step5',
    )
    # get all-form TB incidence
    incidence_actb_agg = (incidence_actb
                          .groupby(['location_id', 'age_group_id', 'sex_id', 'year_id', 'measure_id', 'metric_id'])
                          .sum()
                          .reset_index())
    incidence_actb_agg.drop(columns=['measure_id', 'metric_id', 'cause_id'], inplace=True)
    return incidence_actb_agg

def pull_extra_pulmonary_tb_frac(location_id: int):
    frac = pd.read_csv(f'/ihme/covariates/ubcov/model/output/56726/draws_temp_0/{location_id}.csv')
    frac = frac[frac.year_id == 2019].reset_index(drop=True)
    frac['age_group_id'] = frac.age_group_id.map({5:1,  6:23,  8:149, 10:150, 12:151, 14:152, 16:153, 18:154})
    return frac

def format_data(data: pd.DataFrame):
    data = data.copy()
    data['age_group_name'] = data.age_group_id.map(age_dict)
    data['sex'] = data.sex_id.map(sex_dict)
    data['year'] = 2019
    data['age_group_start'] = data.age_group_name.map(lambda x: int(x.split('_to_')[0]))
    data['age_group_end'] = data.age_group_name.map(lambda x: int(x.split('_to_')[1]))
    data.drop(columns=['age_group_id', 'age_group_name', 'sex_id', 'year_id'], inplace=True)
    data_long = data.melt(id_vars=['location_id', 'age_group_start', 'age_group_end', 'sex', 'year'], var_name=['draw'])
    data_long['draw'] = data_long.draw.map(lambda x: int(x.split('_')[1]))
    return data_long

def get_pulmonary_tb_incidence(incidence_actb: pd.DataFrame, frac: pd.DataFrame):
    incidence = format_data(incidence_actb).set_index(['location_id', 'age_group_start', 'age_group_end', 'sex', 'year', 'draw']) 
    prop = format_data(frac).set_index(['location_id', 'age_group_start', 'age_group_end', 'sex', 'year', 'draw'])
    incidence_pulmonary_tb = incidence * (1 - prop)
    return incidence_pulmonary_tb.reset_index()

def merge_location_data():
    hh_data = pd.DataFrame()
    incidence_data = pd.DataFrame()
    for location_id in country_dict.keys():
        df_hh = load_hh_data(location_id)
        df_hh['location_id'] = location_id
        hh_data = pd.concat([hh_data, df_hh], ignore_index=True)
        incidence_actb = pull_actb_incidence(location_id)
        frac = pull_extra_pulmonary_tb_frac(location_id) 
        incidence_pulmonary_tb = get_pulmonary_tb_incidence(incidence_actb, frac)
        incidence_data = pd.concat([incidence_data, incidence_pulmonary_tb], ignore_index=True)
    return hh_data, incidence_data


if __name__ == '__main__':
    hh_data, incidence_data = merge_location_data()
    hh_data.to_csv('/home/j/Project/simulation_science/latent_tuberculosis_infection/hh_model_input/hh_data.csv', index=False)
    incidence_data.to_csv('/home/j/Project/simulation_science/latent_tuberculosis_infection/hh_model_input/incidence_data.csv', index=False)

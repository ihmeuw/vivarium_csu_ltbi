import numpy as np
import pandas as pd
from vivarium.interpolation import Interpolation

input_dir = '/home/j/Project/simulation_science/latent_tuberculosis_infection/hh_model_input/'
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

def interpolation(incidence_pulmonary_tb: pd.DataFrame, hh_data: pd.DataFrame, draw: int):
    """assign the probability of incident active pulmonary TB for each household member"""
    mask = hh_data.copy()
    interp = Interpolation(incidence_pulmonary_tb.query(f'draw == {draw}'),
                           categorical_parameters=['sex'],
                           continuous_parameters=[['age', 'age_group_start', 'age_group_end']],
                           order=0,
                           extrapolate=True,
                           validate=True)
    mask['pr_pulmonary_tb'] = interp(mask).value
    return mask

def calc_pr_no_pulmonary_tb_in_hh(df: pd.DataFrame):
    """compute the probability that no person with incident active pulmonary TB in a single household"""
    pr_no_tb = 1 - df.pr_pulmonary_tb
    pr_no_tb_in_hh = np.prod(pr_no_tb)
    return pr_no_tb_in_hh

def calc_age_sex_specific_no_pulmonary_tb_prop(df: pd.DataFrame):
    """
    for certain age and sex group
    calculate the probability of no incident active pulmonary TB case in the household
    """
    result = pd.DataFrame()
    age_bins = [0, 5] + list(range(15, 70, 10)) + [125]
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

def get_draw_estimates(file_path: str, draw: int):
    output = pd.DataFrame()
    cols = ['age_group_start', 'age_group_end', 'sex']
    hh_data = pd.read_csv(file_path + 'hh_data.csv')
    incidence_data = pd.read_csv(file_path + 'incidence_data.csv')
    for location_id in country_dict.keys():
        df_hh = hh_data.loc[hh_data.location_id == location_id]
        hh_ids = df_hh.hh_id.unique()
        # boostrap HH data by resampling hh_id with replacement
        sample_hhids = np.random.choice(hh_ids, size=min(len(hh_ids), 50000), replace=True)
        df_hh_sample = df_hh.set_index("hh_id").loc[sample_hhids].reset_index()
        df_incidence = incidence_data.loc[incidence_data.location_id == location_id]
        interp = interpolation(df_incidence, df_hh_sample, draw)
        result = calc_age_sex_specific_no_pulmonary_tb_prop(interp)
        df_incidence_draw = df_incidence.query(f'draw == {draw}')
        df_incidence_draw['value'] = 1 - df_incidence_draw['value']
        # pr_hhc = 1 - pr_no_pulmonary_tb_in_hh / pr_susceptible_to_pulmonary_tb
        f = (1 - result.set_index(cols).value / df_incidence_draw.set_index(cols).value).reset_index()
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
    output = get_draw_estimates(input_dir, draw)
    output.to_hdf(f'/share/scratch/users/yongqx2/hh_incident_pulmonary_tb_estimates/draw_{draw}.hdf', 'draw')

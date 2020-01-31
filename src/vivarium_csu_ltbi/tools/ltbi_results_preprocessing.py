import pandas as pd

result_dir = '/ihme/costeffectiveness/results/vivarium_csu_ltbi/'

path_for_location = {
    'ethiopia_end_100': result_dir + 'hhtb_correlated_ltbi/ethiopia/2020_01_26_12_52_32',
    'india_end_100': result_dir + 'hhtb_correlated_ltbi/india/2020_01_26_12_52_32',
    'peru_end_100': result_dir + 'hhtb_correlated_ltbi/peru/2020_01_26_12_52_32',
    'philippines_end_100': result_dir + 'hhtb_correlated_ltbi/philippines/2020_01_26_12_52_35',
    'south_africa_end_100': result_dir + 'hhtb_correlated_ltbi/south_africa/2020_01_26_12_52_48',
    'ethiopia_end_10': result_dir + 'hhtb_correlated_ltbi_end_age_10/ethiopia/2020_01_29_14_18_32',
    'india_end_10': result_dir + 'hhtb_correlated_ltbi_end_age_10/india/2020_01_29_14_18_32',
    'peru_end_10': result_dir + 'hhtb_correlated_ltbi_end_age_10/peru/2020_01_29_14_18_32',
    'philippines_end_10': result_dir + 'hhtb_correlated_ltbi_end_age_10/philippines/2020_01_29_14_18_34',
    'south_africa_end_10': result_dir + 'hhtb_correlated_ltbi_end_age_10/south_africa/2020_01_29_14_18_36',
}

output_dir = '/home/j/Project/simulation_science/latent_tuberculosis_infection/result/sim_raw_hdf/'

def load_data(country_path):
    df = pd.read_hdf(country_path + '/output.hdf')
    df = df.drop(columns='random_seed').reset_index()
    df.rename(columns={'ltbi_treatment_scale_up.scenario': 'scenario'},
              inplace=True)
    
    scenario_count = (df
                      .groupby(['input_draw', 'random_seed'])
                      .scenario
                      .count())
    idx_completed = scenario_count[scenario_count == 3].reset_index()
    idx_completed = idx_completed[['input_draw', 'random_seed']]
    df_completed = pd.merge(df,
                            idx_completed,
                            how='inner',
                            on=['input_draw', 'random_seed'])
    df_completed = df_completed.groupby(['input_draw', 'scenario']).sum()
    
    return df_completed

def get_year_from_template(template_string):
    return template_string.split('_among_')[0].split('_')[-1]

def get_sex_from_template(template_string):
    return template_string.split('_among_')[1].split('_in_')[0]

def get_age_group_from_template(template_string):
    return '_'.join(template_string.split('_age_group_')[1].split('_treatment_group_')[0].split('_')[:-3])

def get_risk_group_from_template(template_string):
    return template_string.split('_to_hhtb_')[0].split('_')[-1]

def get_treatment_group_from_template(template_string):
    return template_string.split('_treatment_group_')[1]

def get_measure_from_template(template_string):
    return template_string.split('_in_')[0]

def format_data(df):
    idx_cols = ['draw', 'scenario', 'treatment_group', 'hhtb', 'age', 'sex', 'year', 'measure']
    items = ['death', 'ylls', 'ylds', 'event_count', 'prevalent_cases', 'person_time', 'population_point_estimate']
    wanted_cols = []
    for i in df.columns:
        for j in items:
            if j in i:
                wanted_cols.append(i)
    
    df = df[wanted_cols]
    df = (df
          .reset_index()
          .melt(id_vars=['input_draw', 'scenario'],
                var_name='label'))
    df['year'] = df.label.map(get_year_from_template)
    df['sex'] = df.label.map(get_sex_from_template)
    df['age'] = df.label.map(get_age_group_from_template)
    df['hhtb'] = df.label.map(get_risk_group_from_template)
    df['treatment_group'] =  df.label.map(get_treatment_group_from_template)
    df['measure'] = df.label.map(get_measure_from_template)
    df = (df
          .rename(columns={'input_draw': 'draw'})
          .drop(columns='label')
          .set_index(idx_cols))

    return df

if __name__ == '__main__':
    for location, path in path_for_location.items():
        df = load_data(path)
        df = format_data(df)
        df.to_hdf(output_dir + f'{location}_indexed.hdf', mode='w', key='data')

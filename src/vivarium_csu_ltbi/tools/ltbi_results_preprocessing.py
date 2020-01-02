import pandas as pd

result_dir = '/ihme/costeffectiveness/results/vivarium_csu_ltbi/updated-input-data/'

# update it as needed
path_for_location = {
    'ethiopia': result_dir + 'ethiopia//2019_12_30_16_29_23',
    'india': result_dir + 'india/2019_12_30_16_30_19',
    'peru': result_dir + 'peru/2019_12_30_16_30_19',
    'philippines': result_dir + 'philippines/2020_01_02_12_05_32',
    'south_africa': result_dir + 'south_africa/2019_12_30_16_30_19'
}

output_dir = '/home/j/Project/simulation_science/latent_tuberculosis_infection/result/sim_raw_hdf/'

def load_data(country_path):
    df = pd.read_hdf(country_path + '/output.hdf')
    df = df.drop(columns='random_seed').reset_index()
    df.rename(columns={
        'configuration.ltbi_treatment_scale_up.scenario': 'scenario',
        'configuration.population.age_end': 'pop_age_end'},
        inplace=True)
    
    scenario_count = (df.groupby(['input_draw', 'random_seed', 'pop_age_end'])
                        .scenario
                        .count())
    idx_completed = scenario_count[scenario_count == 3].reset_index()
    idx_completed = idx_completed[['input_draw', 'random_seed', 'pop_age_end']]
    df_completed = pd.merge(df,
                            idx_completed,
                            how='inner',
                            on=['input_draw', 'random_seed', 'pop_age_end'])
    
    df_completed = df_completed.groupby(['input_draw', 'pop_age_end', 'scenario']).sum()
    
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

def split_data(df):
    items = ['death', 'ylls', 'ylds', 'event_count', 'prevalent_cases', 'person_time', 'population_point_estimate']
    wanted_cols = []
    for i in df.columns:
        for j in items:
            if j in i:
                wanted_cols.append(i)
    
    df = df[wanted_cols]
    df = (df.reset_index()
            .melt(id_vars=['input_draw', 'pop_age_end', 'scenario'],
                  var_name='label'))
    df['year'] = df.label.map(get_year_from_template)
    df['sex'] = df.label.map(get_sex_from_template)
    df['age'] = df.label.map(get_age_group_from_template)
    df['hhtb'] = df.label.map(get_risk_group_from_template)
    df['treatment_group'] =  df.label.map(get_treatment_group_from_template)
    df['measure'] = df.label.map(get_measure_from_template)
    df = (df.rename(columns={'input_draw': 'draw'})
            .drop(columns='label'))
    
    idx_cols = ['draw', 'scenario', 'treatment_group', 'hhtb', 'age', 'sex', 'year', 'measure']
    df_age_end_5 = (df.loc[df.pop_age_end == 5]
                      .drop(columns='pop_age_end')
                      .set_index(idx_cols))
    df_age_end_100 = (df.loc[df.pop_age_end == 100]
                        .drop(columns='pop_age_end')
                        .set_index(idx_cols))

    return df_age_end_5, df_age_end_100

if __name__ == '__main__':
    for location, path in path_for_location.items():
        df = load_data(path)
        df_age_end_5, df_age_end_100 = split_data(df)
        df_age_end_5.to_hdf(output_dir + f'{location}_indexed_age_end_5.hdf', key='data')
        df_age_end_100.to_hdf(output_dir + f'{location}_indexed_age_end_100.hdf', key='data')

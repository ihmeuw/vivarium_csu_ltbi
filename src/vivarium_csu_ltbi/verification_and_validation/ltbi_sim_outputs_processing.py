import pandas as pd
import yaml
import warnings

result_dir = '/share/costeffectiveness/results/vivarium_csu_ltbi/subminimal-with-risk/'

# update it as needed
path_for_location = {'Ethiopia': result_dir + 'ethiopia/2019_12_02_11_21_29',
                     'India': result_dir + 'india/2019_12_02_11_21_32',
                     'Peru': result_dir + 'peru/2019_12_02_11_21_56',
                     'Philippines': result_dir + 'philippines/2019_12_02_11_21_59',
                     'South Africa': result_dir + 'south_africa/2019_12_02_11_22_06'}

cause_names = ['ltbi_susceptible_hiv', 'activetb_susceptible_hiv', 'protected_tb_susceptible_hiv',
               'ltbi_positive_hiv', 'activetb_positive_hiv', 'protected_tb_positive_hiv',
               'susceptible_tb_positive_hiv', 'other_causes']

template_cols = ['cause', 'sex', 'age_group', 'risk_group', 'measure', 'input_draw']

def load_data(country_path: str):
    """load data and drop uncompleted draws if exists"""
    with open(country_path + '/keyspace.yaml', 'r') as f:
        keyspace = yaml.load(f.read())

    count = len(keyspace['random_seed'])
    df = pd.read_hdf(country_path + '/output.hdf')
    random_seed_count = df.groupby('input_draw').random_seed.count()
    unwanted_draw = list(random_seed_count[random_seed_count != count].index)
    
    df = df.loc[~df.input_draw.isin(unwanted_draw)]
    df = df.groupby('input_draw').sum()
    
    return df

def get_sex_from_template(template_string: str):
    return template_string.split('_among_')[1].split('_in_')[0].capitalize()

def get_age_group_from_template(template_string: str):
    return '_'.join(template_string.split('_age_group_')[1].split('_')[:-3])

def get_risk_group_from_template(template_string: str):
    return '_'.join(template_string.split('_')[-3:])

def standardize_shape(data: pd.DataFrame, measure: str):
    """select specific measure of data and unpivot it into long-format dataframe"""
    if 'due_to' in measure:
        measure_data = data[[c for c in data.columns if measure in c]]
        measure_data = measure_data.reset_index().melt(id_vars='input_draw', var_name='label')
        measure, cause = measure.split('_due_to_', 1)
        measure_data['measure'] = measure
        measure_data['cause'] = cause
    else:
        measure_data = data.loc[:, data.columns.str.startswith('person_time')]
        measure_data = measure_data.reset_index().melt(id_vars=['input_draw'], var_name=['label'])
        measure_data['measure'] = measure
    
    measure_data['sex'] = measure_data.label.map(get_sex_from_template)
    measure_data['age_group'] = measure_data.label.map(get_age_group_from_template)
    measure_data['risk_group'] = measure_data.label.map(get_risk_group_from_template)
    
    measure_data.drop(columns='label', inplace=True)
    
    return measure_data

def get_disaggregated_results(data: pd.DataFrame, cause_names: list):
    """get disaggregated deaths, ylls, ylds, and dalys for each state"""
    deaths = []
    ylls = []
    ylds = []
    dalys = []
    for cause in cause_names:
        deaths.append(standardize_shape(data, f'death_due_to_{cause}'))
        ylls_sub = standardize_shape(data, f'ylls_due_to_{cause}')
        ylls.append(ylls_sub)
        if cause != 'other_causes':
            ylds_sub = standardize_shape(data, f'ylds_due_to_{cause}')    
            dalys_sub = (ylds_sub.set_index([c for c in template_cols if c != 'measure']) + \
                         ylls_sub.set_index([c for c in template_cols if c != 'measure'])).reset_index()
            dalys_sub['measure'] = 'dalys'
            ylds.append(ylds_sub)
            dalys.append(dalys_sub)
        else: # DALYs = YLLs if no YLDs for other_causes
            dalys_sub = (ylls_sub.set_index([c for c in template_cols if c != 'measure'])).reset_index()
            dalys_sub['measure'] = 'dalys'
            dalys.append(dalys_sub)
    
    death_data = pd.concat(deaths)
    yll_data = pd.concat(ylls)
    yld_data = pd.concat(ylds)
    daly_data = pd.concat(dalys)
    
    output = pd.concat([death_data, yll_data, yld_data, daly_data])
    output = output.set_index(template_cols).sort_index()
    
    return output.reset_index()

def append_demographic_aggregates(data: pd.DataFrame, by_cause=False):
    """aggregate results on demographic groups and append it to input data"""
    extra_cols = ['cause'] if by_cause else []
    
    sex_aggregate = data.groupby(extra_cols + ['age_group', 'risk_group', 'measure', 'input_draw']).value.sum().reset_index()
    sex_aggregate['sex'] = 'Both'

    data = pd.concat([data, sex_aggregate])
    
    age_aggregate = data.groupby(extra_cols + ['sex', 'risk_group', 'measure', 'input_draw']).value.sum().reset_index()
    age_aggregate['age_group'] = 'all_ages'

    data = pd.concat([data, age_aggregate])
    
    risk_aggregate = data.groupby(extra_cols + ['sex', 'age_group', 'measure', 'input_draw']).value.sum().reset_index()
    risk_aggregate['risk_group'] = 'all_population'

    data = pd.concat([data, risk_aggregate])
    
    data = data.set_index(extra_cols + template_cols[1:]).sort_index()
    
    return data.reset_index()

def append_cause_aggregates(data: pd.DataFrame):
    """aggregate results on cause and append it to input data"""
    cause_aggregate = data.groupby(template_cols[1:]).value.sum().reset_index()
    cause_aggregate['cause'] = 'all_causes'
    
    data = pd.concat([data, cause_aggregate])
    
    return data.set_index(template_cols).sort_index().reset_index()

def get_person_time(data: pd.DataFrame):
    """pull person time measure and append demographic aggregates"""
    pt = standardize_shape(data, 'person_time')
    pt = append_demographic_aggregates(pt, by_cause=False)
    pt = pt.drop(columns='measure').rename(columns={'value': 'person_time'})
    return pt

def get_table_shell(results: pd.DataFrame, person_time: pd.DataFrame):
    """convert count space results to rate space,
    then calculate mean, lower bound, and upper bound
    """
    results_w_pt = pd.merge(results, person_time, on=['sex', 'age_group', 'risk_group', 'input_draw'])
    results_w_pt.rename(columns={'value': 'count'}, inplace=True)
    results_w_pt['rate'] = results_w_pt['count'] / results_w_pt['person_time'] * 100_000
    
    g = results_w_pt.groupby(template_cols[:-1])[['count', 'rate', 'person_time']].describe(percentiles=[.025, .975])
    t = g.loc[:, pd.IndexSlice[:, ['mean', '2.5%', '97.5%']]]
    return t

def get_hiv_specific_measure(data: pd.DataFrame, name1: str, name2: str, measure_type: str, hiv_status: str):
    """pull hiv-specific activetb counts or ltbi person time"""
    df = data[[c for c in data.columns if name1 in c and name2 in c]]
    df = df.reset_index().melt(id_vars='input_draw', var_name='label')
    
    if measure_type == 'counts':
        df['cause'] = 'activetb'
        df['age_group'] = df.label.map(lambda x: x.split('_age_group_')[1].split('_from_')[0])
    if measure_type == 'person_time':
        df['cause'] ='ltbi'
        df['age_group'] = df.label.map(get_age_group_from_template)
    
    #df['sex'] = df.label.map(get_sex_from_template)
    df['risk_group'] = df.label.map(get_risk_group_from_template)
    df['measure'] = measure_type
    df['hiv_status'] = hiv_status
    
    df.drop(columns='label', inplace=True)
    
    return df

def get_type_specific_results(measure_type: str, col_names: list):
    """sum up hiv positive and hiv negative measure,
    then append demographic aggregates to it
    """
    if measure_type == 'activetb_counts':
        positive_hiv = get_hiv_specific_measure(df, 'ltbi_positive_hiv', 'activetb_positive_hiv', 'counts', 'positive').set_index(col_names)
        susceptible_hiv = get_hiv_specific_measure(df, 'ltbi_susceptible_hiv', 'activetb_susceptible_hiv', 'counts', 'susceptible').set_index(col_names)
    if measure_type == 'ltbi_person_time':
        positive_hiv = get_hiv_specific_measure(df, 'ltbi_positive_hiv', 'person_time', 'person_time', 'positive').set_index(col_names)
        susceptible_hiv = get_hiv_specific_measure(df, 'ltbi_susceptible_hiv', 'person_time', 'person_time', 'susceptible').set_index(col_names)
    
    total = positive_hiv + susceptible_hiv
    total = total.drop(columns='hiv_status').reset_index()
    #total = append_demographic_aggregates(total, by_cause=True)
    
    return total


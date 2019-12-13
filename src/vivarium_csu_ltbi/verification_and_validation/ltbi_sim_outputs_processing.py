import yaml
import warnings
import pandas as pd
from db_queries import get_ids

# update it as needed
result_dir = '/share/costeffectiveness/results/vivarium_csu_ltbi/full-model/'

path_for_location = {'Ethiopia': result_dir + 'ethiopia/2019_12_07_02_10_18',
                     'India': result_dir + 'india/2019_12_07_02_10_34',
                     'Peru': result_dir + 'peru/2019_12_07_02_11_30',
                     'Philippines': result_dir + 'philippines/2019_12_07_02_11_44',
                     'South Africa': result_dir + 'south_africa/2019_12_07_02_12_09'}

cause_names = ['susceptible_tb_susceptible_hiv', 'ltbi_susceptible_hiv', 'activetb_susceptible_hiv',
               'susceptible_tb_positive_hiv', 'ltbi_positive_hiv', 'activetb_positive_hiv',
               'other_causes']

template_cols = ['cause', 'year', 'sex', 'age_group', 'risk_group', 'hiv_status', 'treatment_group', 'measure', 'scenario', 'input_draw']
col_names = ['cause', 'year', 'sex', 'age_group', 'population_subgroup', 'treatment_group', 'measure', 'scenario', 'input_draw']

def load_data(country_path: str):
    """load data and select completed input_draw and random_seed pair"""
    df = pd.read_hdf(country_path + '/output.hdf')
    df = df.drop(columns='random_seed').reset_index()
    df.rename(columns={'ltbi_treatment_scale_up.scenario': 'scenario'}, inplace=True)
    
    scenario_count = df.groupby(['input_draw', 'random_seed']).scenario.count()
    idx_completed = scenario_count[scenario_count == 3].reset_index()[['input_draw', 'random_seed']]
    df_completed = pd.merge(df, idx_completed, how='inner', on=['input_draw', 'random_seed'])
    
    df_completed = df_completed.groupby(['input_draw', 'scenario']).sum()
    
    return df_completed

def get_year_from_template(template_string):
    return template_string.split('_in_')[1].split('_among_')[0]

def get_sex_from_template(template_string):
    return template_string.split('_among_')[1].split('_in_')[0]

def get_age_group_from_template(template_string):
    return '_'.join(template_string.split('_age_group_')[1].split('_treatment_group_')[0].split('_')[:-3])

def get_risk_group_from_template(template_string):
    return '_'.join(template_string.split('_treatment_group_')[0].split('_')[-3:])

def get_treatment_group_from_template(template_string):
    return template_string.split('_treatment_group_')[1]

def get_hiv_status(cause_names: list):
	"""map cause_name to its hiv_status"""
    causes = []
    status = []
    for c in cause_names:
        if 'positive_hiv' in c:
            val = 'positive'
        else:
            val = 'susceptible'
        causes.append(c)
        status.append(val)
    hiv_status = dict(zip(causes, status))
    return hiv_status

def standardize_shape(data: pd.DataFrame, measure: str):
    """select specific measure of data and unpivot it into long-format dataframe
    input measure = {measure}_due_to_{disease_state} or {disease_state}_person_time
    """
    measure_data = data[[c for c in data.columns if measure in c]]
    measure_data = measure_data.reset_index().melt(id_vars=['input_draw', 'scenario'], var_name='label')
    
    if 'due_to' in measure:
        measure, cause = measure.split('_due_to_')
        measure_data['cause'] = cause
        measure_data['measure'] = measure
    else:
        measure_data['cause'] = measure.split('_person_time')[0]
        measure_data['measure'] = 'person_time'
    
    measure_data['hiv_status'] = measure_data.cause.map(get_hiv_status(cause_names))
    
    measure_data['year'] = measure_data.label.map(get_year_from_template)
    measure_data['sex'] = measure_data.label.map(get_sex_from_template)
    measure_data['age_group'] = measure_data.label.map(get_age_group_from_template)
    measure_data['risk_group'] = measure_data.label.map(get_risk_group_from_template)
    measure_data['treatment_group'] =  measure_data.label.map(get_treatment_group_from_template)
    
    measure_data.drop(columns='label', inplace=True)
    
    return measure_data

def get_disaggregated_results(data: pd.DataFrame, cause_names: list):
    """get disaggregated deaths, ylls, ylds, and dalys for each cause"""
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

def format_for_population_subgroup(data: pd.DataFrame):
    """select three sub-population groups:
    all_population, exposed_to_hhtb, and plwhiv
    """
    all_pop = data.groupby([c for c in template_cols if c not in ['risk_group', 'hiv_status']]).sum().reset_index()
    all_pop['population_subgroup'] = 'all_population'
    
    exposure_pop = data.groupby([c for c in template_cols if c != 'hiv_status']).sum().reset_index()
    exposure_pop = exposure_pop.rename(columns={'risk_group': 'population_subgroup'})
    
    hiv_pop = data.groupby([c for c in template_cols if c != 'risk_group']).sum().reset_index()
    hiv_pop = hiv_pop.rename(columns={'hiv_status': 'population_subgroup'})
    
    df = pd.concat([all_pop, exposure_pop, hiv_pop])
    df_sub = df.loc[df.population_subgroup.isin(['all_population', 'exposed_to_hhtb', 'positive'])]
    df_sub['population_subgroup'] = df_sub.population_subgroup.replace('positive', 'plwhiv')
    
    return df_sub

def append_demographic_aggregates(data: pd.DataFrame, by_cause=True):
    """aggregate results on demographic groups and append it to input data"""
    extra_cols = ['cause'] if by_cause else []
    
    year_aggregate = data.groupby(extra_cols + [c for c in col_names[1:] if c != 'year']).sum().reset_index()
    year_aggregate['year'] = 'all_years'
    data = pd.concat([data, year_aggregate])

    sex_aggregate = data.groupby(extra_cols + [c for c in col_names[1:] if c != 'sex']).sum().reset_index()
    sex_aggregate['sex'] = 'both'
    data = pd.concat([data, sex_aggregate])
    
    age_aggregate = data.groupby(extra_cols + [c for c in col_names[1:] if c != 'age_group']).sum().reset_index()
    age_aggregate['age_group'] = 'all_ages'
    data = pd.concat([data, age_aggregate])
    
    data = data.set_index(extra_cols + col_names[1:]).sort_index()
    
    return data.reset_index()

def append_cause_aggregates(data: pd.DataFrame):
    """aggregate results on cause and append it to input data"""
    cause_aggregate = data.groupby(col_names[1:]).sum().reset_index()
    cause_aggregate['cause'] = 'all_causes'
    
    data = pd.concat([data, cause_aggregate])
    
    return data.set_index(col_names).sort_index().reset_index()

def filter_by_causes(data: pd.DataFrame):
    """calculate hiv_other by sum up two states:
    susceptible_tb_positive_hiv and ltbi_positive_hiv,
    calculate activetb by sum up two states:
    activetb_susceptible_hiv and activetb_positive_hiv;
    then select wanted causes.
    """
    wanted_causes = ['all_causes', 'activetb', 'hiv_aids_resulting_in_other_diseases', 'other_causes']
    
    susceptible_tb_positive_hiv = data.loc[data.cause == 'susceptible_tb_positive_hiv'].set_index(col_names[1:])
    ltbi_positive_hiv = data.loc[data.cause == 'ltbi_positive_hiv'].set_index(col_names[1:])
    hiv_other = susceptible_tb_positive_hiv + ltbi_positive_hiv
    hiv_other['cause'] = 'hiv_aids_resulting_in_other_diseases'
    
    activetb_susceptible_hiv = data.loc[data.cause == 'activetb_susceptible_hiv'].set_index(col_names[1:])
    activetb_positive_hiv = data.loc[data.cause == 'activetb_positive_hiv'].set_index(col_names[1:])
    activetb = activetb_susceptible_hiv + activetb_positive_hiv
    activetb['cause'] = 'activetb'
    
    data = pd.concat([data, hiv_other.reset_index(), activetb.reset_index()])
    data = data.loc[data.cause.isin(wanted_causes)]
    data = data.set_index(col_names).sort_index()
    
    return data.reset_index()

def get_person_time(data: pd.DataFrame):
    """pull person time measure, sum up by disease states,
    then append demographic aggregates.
    """
    person_time = pd.DataFrame()
    for cause in cause_names:
        pt = standardize_shape(data, f'{cause}_person_time')
        person_time = pd.concat([person_time, pt])
    
    pt_agg = format_for_population_subgroup(person_time).groupby(col_names[1:]).sum().reset_index()
    pt_agg = append_demographic_aggregates(pt_agg, by_cause=False)
    pt_agg = pt_agg.drop(columns='measure').rename(columns={'value': 'person_time'})
    
    return pt_agg

def get_table_shell(results: pd.DataFrame, person_time: pd.DataFrame):
    """calculate averted value and convert everything to rate space;
    then summarize mean, lower bound, and upper bound.
    """
    results_w_pt = pd.merge(results, person_time, how='inner',
                            on=['year', 'sex', 'age_group', 'population_subgroup', 'treatment_group', 'scenario', 'input_draw'])
    bau = results_w_pt.loc[results_w_pt.scenario == 'baseline'].drop(columns=['scenario', 'person_time'])
    
    t = pd.merge(results_w_pt, bau, how='inner',
                 on=[c for c in col_names if c != 'scenario'],
                 suffixes=['', '_bau'])
    t['averted'] = t['value_bau'] - t['value']
    t.drop(columns='value_bau', inplace=True)
    t['value'] =  t['value'] / t['person_time'] * 100_000
    t['averted'] = t['averted'] / t['person_time'] * 100_000
    t = t.fillna(0.0)
    
    g = t.groupby(col_names[:-1])[['value', 'averted', 'person_time']].describe(percentiles=[.025, .975])
    g = g.loc[:, pd.IndexSlice[:, ['mean', '2.5%', '97.5%']]]
    g.columns = [f'{outcome}_{val}' for outcome, val in zip(g.columns.get_level_values(0), g.columns.get_level_values(1))]

    return g.reset_index()

def get_age_group_dict():
    """pairwise age group name and age group id"""
    age_group_ids = list(range(2, 21)) + [30, 31, 32, 235]

    age_table = get_ids('age_group')
    age_table = age_table[age_table.age_group_id.isin(age_group_ids)]
    age_table['age_group_name'] = age_table.age_group_name.map(lambda x: x.replace(' ', '_').lower())

    age_group_dict = dict(zip(age_table.age_group_name, age_table.age_group_id))
    age_group_dict.update({'all_ages': 1})
    
    return age_group_dict

def format_for_table_shell(data: pd.DataFrame, location_name: str, age_group_dict: dict):
    """sort the non-value index columns for table shell"""
    age_group_dict_swap = dict((v, k) for k, v in age_group_dict.items())
    
    df = data.copy()
    df['location'] = location_name
    df['age_group'] = df.age_group.map(age_group_dict)
    df = df.set_index(['location'] + col_names[:-1]).sort_index().reset_index()
    df['age_group'] = df.age_group.map(age_group_dict_swap)
    
    return df

def get_country_estimates(location_name: str, path: str, cause_names: list):
    """wrap up post-processing functions to generate country-specific estimates"""
    df = load_data(path)
    output = get_disaggregated_results(df, cause_names[2:])
    output = format_for_population_subgroup(output)
    results = append_cause_aggregates(append_demographic_aggregates(output))
    results = filter_by_causes(results)
    pt = get_person_time(df, cause_names[:-1])
    table_shell = get_table_shell(results, pt)
    table_shell = format_for_table_shell(table_shell, location_name, get_age_group_dict())
    
    return table_shell

def get_hiv_specific_measure(data: pd.DataFrame, name1: str, name2: str, cause_name: str, measure_type: str, hiv_status: str):
    """pull hiv-specific activetb counts or ltbi person time"""
    df = data[[c for c in data.columns if name1 in c and name2 in c]]
    df = df.reset_index().melt(id_vars=['input_draw', 'scenario'], var_name='label')
    
    df['cause'] = cause_name
    df['year'] = df.label.map(get_year_from_template) 
    df['sex'] = df.label.map(get_sex_from_template)
    df['age_group'] = df.label.map(get_age_group_from_template)
    df['risk_group'] = df.label.map(get_risk_group_from_template)
    df['hiv_status'] = hiv_status
    df['treatment_group'] = df.label.map(get_treatment_group_from_template)
    df['measure'] = measure_type
    
    df.drop(columns='label', inplace=True)
    
    return df

def get_type_specific_results(data: pd.DataFrame, measure_type: str):
    """measure_type = activetb_incidence_count or ltbi_person_time"""
    if measure_type == 'activetb_incidence_count':
        positive_hiv = get_hiv_specific_measure(data, 'ltbi_positive_hiv', 'activetb_positive_hiv', 'activetb', 'incidence_count', 'positive')
        susceptible_hiv = get_hiv_specific_measure(data, 'ltbi_susceptible_hiv', 'activetb_susceptible_hiv', 'activetb', 'incidence_count', 'susceptible')
    if measure_type == 'ltbi_person_time':
        positive_hiv = get_hiv_specific_measure(data, 'ltbi_positive_hiv', 'person_time', 'ltbi', 'person_time', 'positive')
        susceptible_hiv = get_hiv_specific_measure(data, 'ltbi_susceptible_hiv', 'person_time', 'ltbi', 'person_time', 'susceptible')
    
    total = pd.concat([positive_hiv, susceptible_hiv])
    total = append_demographic_aggregates(format_for_population_subgroup(total))

    return total

def get_activetb_incidence(data: pd.DataFrame):
    """activetb_incidence_rate = activetb_incidence_count / ltbi_person_time"""
    activetb_incidence_count = get_type_specific_results(data, 'activetb_incidence_count')
    ltbi_person_time = get_type_specific_results(data, 'ltbi_person_time')
    
    numerator = activetb_incidence_count.set_index([c for c in col_names if c not in ['cause', 'measure']])['value']
    denominator = ltbi_person_time.set_index([c for c in col_names if c not in ['cause', 'measure']])['value']
    activetb_incidence_rate = numerator / denominator * 100_000
    activetb_incidence_rate = activetb_incidence_rate.fillna(0.0).reset_index()
    activetb_incidence_rate['cause'] = 'activetb'
    activetb_incidence_rate['measure'] = 'incidence_rate'
    
    activetb_incidence = pd.concat([activetb_incidence_count, activetb_incidence_rate])
    activetb_incidence = activetb_incidence.set_index(col_names).sort_index()
    
    return activetb_incidence.reset_index()

def get_activetb_incidence_averted(data: pd.DataFrame):
	"""calculate averted results for activetb"""
    bau = data.loc[data.scenario == 'baseline'].drop(columns='scenario')

    t = pd.merge(data, bau, how='inner',
                 on=[c for c in col_names if c != 'scenario'],
                 suffixes=['', '_bau'])
    t['averted'] = t['value_bau'] - t['value']
    t.drop(columns='value_bau', inplace=True)

    g = t.groupby(col_names[:-1])[['value', 'averted']].describe(percentiles=[.025, .975])
    g = g.loc[:, pd.IndexSlice[:, ['mean', '2.5%', '97.5%']]]
    g.columns = [f'{outcome}_{val}' for outcome, val in zip(g.columns.get_level_values(0), g.columns.get_level_values(1))]
    
    return g.reset_index()


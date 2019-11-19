import pandas as pd
import numpy as np

import yaml
import warnings

result_dir = '/share/costeffectiveness/vivarium_csu_ltbi/'
country_names = ['brazil', 'ethiopia', 'india', 'philippines', 'south_africa']

path_for_location = {'brazil': result_dir + 'brazil/2019_11_12_22_39_17',
					 'ethiopia': result_dir + 'ethiopia/2019_11_12_19_23_10',
					 'india': result_dir + 'india/2019_11_12_19_20_29',
					 'philippines': result_dir + 'philippines/2019_11_12_22_40_56',
					 'south_africa': result_dir + 'south_africa/2019_11_12_19_24_48'}

cause_names = ['ltbi_susceptible_hiv', 'activetb_susceptible_hiv', 'protected_tb_susceptible_hiv',
			   'ltbi_positive_hiv', 'activetb_positive_hiv', 'protected_tb_positive_hiv',
			   'susceptible_tb_positive_hiv', 'other_causes']

template_cols = ['cause', 'sex', 'age_group', 'measure', 'input_draw']

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

def get_sex_from_template(template_string):
	return template_string.split('_among_')[1].split('_in_')[0].capitalize()

def get_age_group_from_template(template_string):
	return template_string.split('_age_group_')[1]

def standardize_shape(data, measure):
	"""select specific measure of data and unpivot it into long-format dataframe"""
	measure_data = data[[c for c in data.columns if measure in c]]
	measure_data = measure_data.reset_index().melt(id_vars=['input_draw'], var_name=['label'])
	
	if 'due_to' in measure:
		measure, cause = measure.split('_due_to_', 1)
		measure_data['measure'] = measure
		measure_data['cause'] = cause
	else:
		measure_data['measure'] = measure  
	measure_data['sex'] = measure_data.label.map(get_sex_from_template)
	measure_data['age_group'] = measure_data.label.map(get_age_group_from_template)
	
	measure_data.drop(columns='label', inplace=True)
	return measure_data

def get_disaggregated_results(data, cause_names):
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

def append_demographic_aggregates(data, by_cause=False):
	"""aggregate results on demographic groups and append it to input data"""
	extra_cols = ['cause'] if by_cause else []
	
	age_aggregate = data.groupby(extra_cols + ['sex', 'measure', 'input_draw']).value.sum().reset_index()
	age_aggregate['age_group'] = 'all_ages'

	data = pd.concat([data, age_aggregate])

	sex_aggregate = data.groupby(extra_cols + ['age_group', 'measure', 'input_draw']).value.sum().reset_index()
	sex_aggregate['sex'] = 'Both'

	data = pd.concat([data, sex_aggregate])
	data = data.set_index(extra_cols + template_cols[1:]).sort_index()
	
	return data.reset_index()

def append_cause_aggregates(data):
	"""aggregate results on cause and append it to input data"""
	cause_aggregate = data.groupby(template_cols[1:]).value.sum().reset_index()
	cause_aggregate['cause'] = 'all_causes'
	
	data = pd.concat([data, cause_aggregate])
	
	return data.set_index(template_cols).sort_index().reset_index()

def get_person_time(data):
	"""aggregate person_time on demographic groups"""
	pt = standardize_shape(data, 'person_time')
	pt_agg = append_demographic_aggregates(pt, by_cause=False)
	pt_agg = pt_agg.drop(columns='measure').rename(columns={'value': 'person_time'})
	
	return pt_agg

def get_table_shell(results: pd.DataFrame, person_time: pd.DataFrame):
	"""convert count space results to rate space and calculate mean, lower bound, and upper bound"""
	results_w_pt = pd.merge(results, person_time, on=['sex', 'age_group', 'input_draw'])
	results_w_pt.rename(columns={'value': 'count'}, inplace=True)
	results_w_pt['rate'] = results_w_pt['count'] / results_w_pt['person_time'] * 100_000
	
	g = results_w_pt.groupby(template_cols[:-1])[['count', 'rate', 'person_time']].describe(percentiles=[.025, .975])
	table_shell = g.filter([('count', 'mean'), ('count', '2.5%'), ('count', '97.5%'),
							('rate', 'mean'), ('rate', '2.5%'), ('rate', '97.5%'),
							('person_time', 'mean'), ('person_time', '2.5%'), ('person_time', '97.5%')])
	return table_shell
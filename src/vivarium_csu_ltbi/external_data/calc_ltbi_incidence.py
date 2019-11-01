import pandas as pd
import numpy as np
import dismod_mr
import pymc as pm
from gbd_mapping import causes
from vivarium_inputs.interface import get_measure

country_names = ['South Africa', 'India', 'Philippines', 'Ethiopia', 'Brazil']
index_cols = ['draw', 'location', 'sex', 'age_group_start', 'age_group_end', 'year_start', 'year_end']
actb_names = ['drug_susceptible_tuberculosis', 
			  'multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
			  'extensively_drug_resistant_tuberculosis',
			  'hiv_aids_drug_susceptible_tuberculosis',
			  'hiv_aids_multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
			  'hiv_aids_extensively_drug_resistant_tuberculosis']

def load_data(country_name: str):
	"""output LTBI prevalence, LTBI excess mortality, and All Causes cause-specific mortality"""
	p_ltbi = get_measure(causes.latent_tuberculosis_infection, 'prevalence', country_name)
	
	i_actb = p_ltbi.drop('value', axis=1)
	i_actb['value'] = 0.0
	i_actb = i_actb.set_index(index_cols)
	# aggregate all child active TB causes incidence to obtain all-form active TB incidence
	for actb in actb_names:
		i_actb += get_measure(getattr(causes, actb), 'incidence', country_name).set_index(index_cols)
	
	f_ltbi = i_actb / p_ltbi.set_index(index_cols)
	f_ltbi = f_ltbi.reset_index()
	csmr_all = get_measure(causes.all_causes, 'cause_specific_mortality', country_name)
	return p_ltbi, f_ltbi, csmr_all

def format_for_dismod(df: pd.DataFrame, draw: int, sex: str, year: int, data_type: str):
	"""prepare data into dismod format"""
	df = df.query(f'draw == {draw} and sex == "{sex}" and year_start == {year}').copy()
	df['data_type'] = data_type
	df['area'] = 'all'
	df['sex'] = 'total'
	df['standard_error'] = 0.01
	df['upper_ci'] = np.nan
	df['lower_ci'] = np.nan
	df['effective_sample_size'] = 1_000
	
	df = df.rename(columns={'age_group_start': 'age_start', 'age_group_end': 'age_end'})
	return df

def make_disease_model(p: pd.DataFrame, f: pd.DataFrame, m_all: pd.DataFrame, knots: list):
	dm = dismod_mr.data.ModelData()
	# prepare dismod input data
	dm.input_data = pd.concat([p, f, m_all], ignore_index=True)
	# set age knots
	for rate_type in 'ifr':
		dm.set_knots(rate_type, knots)
	# set proper value and boundaries for dismod input measures
	dm.set_level_bounds('i', lower=1e-3, upper=1e-1)
	dm.set_level_bounds('f', lower=1e-4, upper=1e-1)
	dm.set_level_value('r', value=0, age_before=101, age_after=101)
	dm.set_level_bounds('r', lower=1e-6, upper=1)
	dm.set_level_value('p', value=.2, age_before=1, age_after=101)
	
	return dm

def fit_and_predict(p: pd.DataFrame, f: pd.DataFrame, m_all: pd.DataFrame, knots: list):
	"""predict LTBI incidence for certain country, sex, and year
	based on single draw input data"""
	dm = make_disease_model(p, f, m_all, knots)
	dm.setup_model(rate_model='normal', include_covariates=False)
	# set all dismod variables to maximum a posteriori values
	m = pm.MAP(dm.vars)
	m.fit()
	 
	i_ltbi = dm.vars['i']['mu_age'].value
	return i_ltbi

def format_for_art(i_ltbi: np.ndarray, draw: int, country_name: str, sex: str, year: int):
	"""prepare LTBI incidence back calculated from dismod into sim data artifact format"""
	df = pd.DataFrame({'value': i_ltbi[:-1]})
	df['draw'] = draw
	df['location'] = country_name
	df['age_group_start'] = np.arange(0, 100)
	df['age_group_end'] = np.arange(1, 101)
	df['sex'] = sex
	df['year_start'] = year
	df['year_end'] = year+1
	df['parameter'] = 'continuous'
	
	return df

def output_incidence_estimates(knots: list):
	"""sweep over country, draw, sex, and year"""
	output = pd.DataFrame()
	for country_name in country_names:
		p_ltbi, f_ltbi, csmr_all = load_data(country_name)
		for draw in range(1000):
			for sex in ['Female', 'Male']:
				for year in range(1990, 2018):
					# convert data into dismod input format
					p = format_for_dismod(p_ltbi, draw, sex, year, 'p')
					f = format_for_dismod(f_ltbi, draw, sex, year, 'f')
					m_all = format_for_dismod(csmr_all, draw, sex, year, 'm_all')
					# back calculate LTBI incidence from dismod
					i_ltbi = fit_and_predict(p, f, m_all, knots)
					df = format_for_art(i_ltbi, draw, country_name, sex, year)
					output = pd.concat([output, df], ignore_index=True)
	return output





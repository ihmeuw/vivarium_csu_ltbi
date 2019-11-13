import pandas as pd
from gbd_mapping import causes
from vivarium.interpolation import Interpolation
from vivarium_inputs.interface import get_measure
from vivarium_inputs.data_artifact.utilities import split_interval

master_dir = '/home/j/Project/simulation_science/latent_tuberculosis_infection/literature/household_structure/'
index_cols = ['draw', 'location', 'sex', 'age_start', 'age_end', 'year_start', 'year_end']
actb_names = ['drug_susceptible_tuberculosis', 
			  'multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
			  'extensively_drug_resistant_tuberculosis',
			  'hiv_aids_drug_susceptible_tuberculosis',
			  'hiv_aids_multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
			  'hiv_aids_extensively_drug_resistant_tuberculosis']

def load_and_transform(country_name: str):
	"""output all-form TB prevalence"""
	prev_ltbi = get_measure(causes.latent_tuberculosis_infection, 'prevalence', country_name)
	
	prev_actb = pd.DataFrame(0.0, index=prev_ltbi.index, columns=['draw_' + str(i) for i in range(1000)])
	# aggregate all child active TB causes prevalence to obtain all-form TB prevalence
	for actb in actb_names:
		prev_actb += get_measure(getattr(causes, actb), 'prevalence', country_name)

	prev_actb = split_interval(prev_actb, interval_column='age', split_column_prefix='age_group')
	prev_actb = split_interval(prev_actb, interval_column='year', split_column_prefix='year')
	prev_actb = prev_actb.stack().reset_index().rename(columns={'level_6': 'draw', 0: 'value'})
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
	"""compute the probability that there is at least one person with active TB for each household"""
	pr_no_tb = 1 - df.pr_actb
	pr_no_tb_in_hh = np.prod(pr_no_tb)
	return 1 - pr_no_tb_in_hh

def age_sex_specific_actb_prop(df: pd.DataFrame):
	"""calculate the probability of an active TB case in the household
	for certain age and sex group
	"""
	age_bins = [0, 1] + list(range(5, 96, 5)) + [125]
	res = pd.DataFrame()
	for i, age_start in enumerate(age_bins[:-1]):
		age_end = age_bins[i+1]
		for sex in ['Male', 'Female']:
			hh_with = df.query(f'age >= {age_start} and age < {age_end} and sex == "{sex}"').hh_id.unique()
			prop = df[df.hh_id.isin(hh_with)].groupby('hh_id').apply(calc_pr_actb_in_hh)
			prop_mean = prop.mean()
			age_sex_res = pd.DataFrame({'age_start': [age_start], 'age_end': [age_end], 
										'sex': [sex], 'pr_actb_in_hh': [prop_mean]})
			res = res.append(age_sex_res, ignore_index=True)
	return res

def country_specific_outputs(country_name: str, year_start=2017):
	"""for certain country in year 2017,
	sweep over draw
	"""
	outputs = pd.DataFrame()
	df_hh = pd.read_csv(master_dir + country_name + '.csv') # TO DO: prepare long-format country-specific HH microdata from the most recent survey
	prev_actb = load_and_transform(country_name)
	for draw in range(1000):
		data = interpolation(prev_actb, df_hh, year_start, draw)
		res = age_sex_specific_actb_prop(data)
		res['location'] = country_name
		res['year_start'] = year_start
		res['year_end'] = year_start + 1
		res['draw'] = draw
		outputs = pd.concat([outputs, res], ignore_index=True)
	
	outputs = outputs.set_index(index_cols)
	return outputs






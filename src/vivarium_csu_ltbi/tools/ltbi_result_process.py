import pandas as pd, numpy as np
import matplotlib.pyplot as plt, scipy.stats as st

def clean_aggregate(data):
    del data['random_seed']
    new_data = data.reset_index().groupby(['input_draw_number', 'ltbi_treatment_scale_up.scenario']).sum()
    new_data.drop(list(new_data.filter(regex = 'person_time')), axis = 1, inplace = True)
    return new_data

def readCleanDf(country, filePath):
    df = pd.read_hdf(filePath)
    df_100 = clean_aggregate(df)
    return country, df_100

def shape(data, pattern):
    df_data = data.filter(regex = pattern)
    new_data = df_data.stack().reset_index().rename(columns = {0:'value', 'level_2' : 'information'})
    new_data['age_group'] = new_data["information"].str.extract('^.*age_group_([a-z]*_?[a-z]*[0-9]*_?t?o?_?[0-9]*)', expand=True)
    new_data['hiv_status'] = new_data["information"].str.extract("activetb_([a-z]*)_hiv", expand=True)
    new_data['hh_status'] = new_data["information"].str.extract("^.*_([a-z]*)_to_hhtb")
    return new_data

def addElement(data, country, measure, people, scenario,mean, lower, upper):
    data.get("country").append(country)
    data.get("measure").append(measure)
    data.get("people").append(people)
    data.get("scenario").append(scenario)
    data.get("mean").append(mean)
    data.get("lower").append(lower)
    data.get("upper").append(upper)
    return

def calculate_mean_ci(df, col_name):
    mean = df[col_name].mean()
    lower = st.t.interval(0.95, len(df[col_name])-1, loc=np.mean(df[col_name]), scale=st.sem(df[col_name]))[0]
    upper = st.t.interval(0.95, len(df[col_name])-1, loc=np.mean(df[col_name]), scale=st.sem(df[col_name]))[1]
    return mean, lower, upper

def u5_calculation(df, data, country, measure, people):
    df_u5= df[df['age_group'].isin(['early_neonatal_', 'late_neonatal_', 'post_neonatal_', '1_to_4']) & df['hh_status'].isin(['exposed'])]
    result = df_u5.groupby(['input_draw_number','ltbi_treatment_scale_up.scenario']).sum().reset_index(level = 1)
    result['3HP_averted'] = (result.loc[result['ltbi_treatment_scale_up.scenario'] == 'baseline', 'value'] - result.loc[result['ltbi_treatment_scale_up.scenario'] == '3HP_scale_up', 'value'])*100/result.loc[result['ltbi_treatment_scale_up.scenario'] == 'baseline', 'value']
    result['6H_averted'] = (result.loc[result['ltbi_treatment_scale_up.scenario'] == 'baseline', 'value'] - result.loc[result['ltbi_treatment_scale_up.scenario'] == '6H_scale_up', 'value'])*100/result.loc[result['ltbi_treatment_scale_up.scenario'] == 'baseline', 'value']
    final_df = result.reset_index()[['input_draw_number','3HP_averted','6H_averted']].drop_duplicates()
    addElement(data, country, measure, people, '6H_averted', calculate_mean_ci(final_df, '6H_averted')[0], calculate_mean_ci(final_df, '6H_averted')[1], calculate_mean_ci(final_df, '6H_averted')[2])
    addElement(data, country, measure, people, '3HP_averted', calculate_mean_ci(final_df, '3HP_averted')[0], calculate_mean_ci(final_df, '3HP_averted')[1], calculate_mean_ci(final_df, '3HP_averted')[2])
    return

def hiv_calculation(df, data, country,  measure, people):
    df_hiv= df[df['hiv_status'].isin(['positive'])]
    result = df_hiv.groupby(['input_draw_number','ltbi_treatment_scale_up.scenario']).sum().reset_index(level = 1)
    result['3HP_averted'] = (result.loc[result['ltbi_treatment_scale_up.scenario'] == 'baseline', 'value'] - result.loc[result['ltbi_treatment_scale_up.scenario'] == '3HP_scale_up', 'value'])*100/result.loc[result['ltbi_treatment_scale_up.scenario'] == 'baseline', 'value']
    result['6H_averted'] = (result.loc[result['ltbi_treatment_scale_up.scenario'] == 'baseline', 'value'] - result.loc[result['ltbi_treatment_scale_up.scenario'] == '6H_scale_up', 'value'])*100/result.loc[result['ltbi_treatment_scale_up.scenario'] == 'baseline', 'value']
    final_df = result.reset_index()[['input_draw_number','3HP_averted','6H_averted']].drop_duplicates()
    addElement(data, country, measure, people, '6H_averted', calculate_mean_ci(final_df, '6H_averted')[0], calculate_mean_ci(final_df, '6H_averted')[1], calculate_mean_ci(final_df, '6H_averted')[2])
    addElement(data, country, measure, people, '3HP_averted', calculate_mean_ci(final_df, '3HP_averted')[0], calculate_mean_ci(final_df, '3HP_averted')[1], calculate_mean_ci(final_df, '3HP_averted')[2])
    return

def process_data(df_total, country, measure, pattern):
    df_100_incidence = shape(df_total, pattern)
    u5_calculation(df_100_incidence, data, country, measure, "U5_hh")
    hiv_calculation(df_100_incidence, data, country, measure, "PLWHIV")
    return


if __name__ == '__main__':
    result_dir = '/ihme/costeffectiveness/results/vivarium_csu_ltbi/'
    loc_filePath = {
        'ethiopia_end_100': result_dir + 'updated-input-data-end-100/ethiopia/2020_01_03_14_22_51/output.hdf',
        'india_end_100': result_dir + 'updated-input-data-end-100/india/2020_01_03_14_23_42/output.hdf',
        'peru_end_100': result_dir + 'updated-input-data-end-100/peru/2020_01_03_14_23_33/output.hdf',
        'philippines_end_100': result_dir + 'updated-input-data-end-100/philippines/2020_01_03_14_23_28/output.hdf',
        'south_africa_end_100': result_dir + 'updated-input-data-end-100/south_africa/2020_01_03_14_23_36/output.hdf'
    }
    measure_pattern = {"incidence": "^ltbi.*activetb", "death": "death_due_to_activetb",
                       "DALYs": "ylds_due_to_activetb|ylls_due_to_activetb"}
    data = {"country": [], "measure": [], "people": [], "scenario": [], "mean": [], "lower": [], "upper": []}
    for location, filePath in loc_filePath.items():
        locDf = readCleanDf(location, filePath)
        for measure, pattern in measure_pattern.items():
            process_data(locDf[1], locDf[0], measure, pattern)

    pd.DataFrame(data).to_csv('/home/j/Project/simulation_science/latent_tuberculosis_infection/result/averted_result.csv')
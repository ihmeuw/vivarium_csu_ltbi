import pandas as pd, numpy as np
from db_queries import get_population

def mergeCountry(df1, df2):
    if "random_seed" in df1.columns:
        del df1['random_seed']
    if "random_seed" in df2.columns:
        del df2['random_seed']
    df = (df1.reset_index().set_index(['input_draw_number', 'random_seed', 'ltbi_treatment_scale_up.scenario'])
              .add(
              df2.reset_index().set_index(['input_draw_number', 'random_seed', 'ltbi_treatment_scale_up.scenario']),
                  fill_value=0,
          ))
    assert not np.any(df.isnull())
    return df.reset_index()

def clean(data):
    del data['random_seed']
    new_data = data.reset_index().groupby(['input_draw_number', 'ltbi_treatment_scale_up.scenario']).sum()
    new_data.drop(list(new_data.filter(like = 'person_time')), axis = 1, inplace = True)
    return new_data

def process(country, df, data, location_dict):
    df_data = df.filter(like = "hhtb", axis = 1)
    new_data = df_data.stack().reset_index().rename(columns = {0:'value', 'level_2' : 'information'})
    new_data['age_group'] = new_data["information"].str.extract('^.*age_group_([a-z]*_?[a-z]*[0-9]*_?t?o?_?[0-9]*)', expand=True)
    new_data['hh_status'] = new_data["information"].str.extract("^.*_([a-z]*)_to_hhtb")
    #calculate proportion
    num_u5 = new_data[new_data['age_group'].isin(['early_neonatal_', 'late_neonatal_', 'post_neonatal_', '1_to_4'])]["value"].sum()
    df_u5= new_data[new_data['age_group'].isin(['early_neonatal_', 'late_neonatal_', 'post_neonatal_', '1_to_4'])].groupby("hh_status").sum().reset_index()
    num_u5_hhtb = df_u5.loc[df_u5["hh_status"] == "exposed","value"][0]
    hhtb_prop = num_u5_hhtb/num_u5
    #rescale to national level
    pop = get_population(location_id= location_dict.get(country),age_group_id=1,sex_id=3,gbd_round_id=5,
	year_id = 2017).population[0]
    num_u5_pop = pop*hhtb_prop
    data.get("country").append(country)
    data.get("num_u5_pop").append(num_u5_pop)
    data.get("hhtb_prop(%)").append(hhtb_prop*100)

if __name__ == '__main__':
    result_dir = '/ihme/costeffectiveness/results/vivarium_csu_ltbi/'
    loc_100_filePath = {
        'ethiopia_end_100': result_dir + 'updated-input-data-end-100/ethiopia/2020_01_03_14_22_51/output.hdf',
        'india_end_100': result_dir + 'updated-input-data-end-100/india/2020_01_03_14_23_42/output.hdf',
        'peru_end_100': result_dir + 'updated-input-data-end-100/peru/2020_01_03_14_23_33/output.hdf',
        'philippines_end_100': result_dir + 'updated-input-data-end-100/philippines/2020_01_03_14_23_28/output.hdf',
        'southAfrica_end_100': result_dir + 'updated-input-data-end-100/south_africa/2020_01_03_14_23_36/output.hdf'
    }
    loc_10_filePath = {
        'ethiopia_end_10': result_dir + 'updated-input-data-end-10/ethiopia/2020_01_02_17_00_21/output.hdf',
        'india_end_10': result_dir + 'updated-input-data-end-10/india/2020_01_02_17_00_23/output.hdf',
        'peru_end_10': result_dir + 'updated-input-data-end-10/peru/2020_01_02_17_01_17/output.hdf',
        'philippines_end_10': result_dir + 'updated-input-data-end-10/philippines/2020_01_02_17_01_17/output.hdf',
        'southAfrica_end_10': result_dir + 'updated-input-data-end-10/south_africa/2020_01_02_17_01_26/output.hdf'
    }
    location_dict = {'ethiopia': 179, 'india': 163, 'peru': 123, 'philippines': 16, 'southAfrica': 196}
    data = {"country": [], "num_u5_pop": [], "hhtb_prop(%)": []}
    loc_df = {}
    for location_100, filePath_100 in loc_100_filePath.items():
        for location_10, filePath_10 in loc_10_filePath.items():
            if location_100.split("_")[0] == location_10.split("_")[0]:
                location = location_100.split("_")[0]
                df_10 = pd.read_hdf(filePath_10)
                df_100 = pd.read_hdf(filePath_100)
                df_merged = mergeCountry(df_100, df_10)
                loc_df[location] = df_merged
    for location, df in loc_df.items():
        process(location, clean(df), data, location_dict)
    pd.DataFrame(data).to_csv('/home/j/Project/simulation_science/latent_tuberculosis_infection/result/count_percent_u5_hh_AcTB.csv')
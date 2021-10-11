import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from db_queries import get_population
from matplotlib.backends.backend_pdf import PdfPages


# GBD top 20 TB incidence (cases) country list
country_names = [
    'India', 'China', 'Nigeria', 'Indonesia', 'Democratic Republic of the Congo',
    'Pakistan', 'South Africa', 'Philippines', 'Ethiopia', 'Bangladesh', 
    'United Republic of Tanzania', 'Mozambique', 'Uganda', 'Kenya', 'Viet Nam', 
    'Russian Federation', 'Zimbabwe', 'Myanmar', 'Angola', 'Zambia'
]

country_dict = {
    6: 'China',
    11: 'Indonesia',
    15: 'Myanmar',
    16: 'Philippines',
    20: 'Vietnam',
    62: 'Russia',
    161: 'Bangladesh',
    163: 'India',
    165: 'Pakistan',
    168: 'Angola',
    171: 'Congo', # Democratic Republic of the Congo
    179: 'Ethiopia',
    180: 'Kenya',
    184: 'Mozambique',
    189: 'Tanzania',
    190: 'Uganda',
    191: 'Zambia',
    196: 'South Africa',
    198: 'Zimbabwe',
    214: 'Nigeria'
}

# GBD age group ids
age_group_ids = [1, 23, 149, 150, 151, 152, 153, 154]
age_group_names = ['0_to_5', '5_to_15'] + [f'{c}_to_{c+10}' for c in range(15, 65, 10)] + ['65_to_125'] 
age_dict = dict(zip(age_group_ids, age_group_names))

sex_dict = {
    1: 'Male',
    2: 'Female'
}

age_group_map = {
    1: '0_to_5',
    23: '5_to_15',
    8: '15_to_20',
    9: '20_to_25',
    10: '25_to_30',
    11: '30_to_35',
    12: '35_to_40',
    13: '40_to_45',
    14: '45_to_50',
    15: '50_to_55',
    16: '55_to_60',
    17: '60_to_65',
    154: '65_to_125'
}

# GBD popualtion size by age and sex
population = get_population(
    location_id=list(country_dict.keys()),
    age_group_id=[1, 23] + list(range(8, 18)) + [154],
    sex_id=[1,2],
    year_id=2019,
    gbd_round_id=6,
    decomp_step='step5'
)
population['age_group'] = population.age_group_id.map(age_group_map)
population['location'] = population.location_id.map(country_dict)
population['sex'] = population.sex_id.map(sex_dict)
population = (population
              .loc[:, ['location', 'age_group', 'sex', 'year_id', 'population']]
              .rename(columns={'age_group': 'age', 'year_id': 'year'}))
population = population.set_index(['location', 'age', 'sex', 'year']).sort_index().reset_index()

def get_pop_agg(data: pd.DataFrame, ages):
    age_aggregates = []
    for group, ages in ages.items():
        age_group = data[data.age.isin(ages)]
        age_group_summary = (age_group
                             .groupby(['location', 'year', 'sex'])
                             .population.sum()
                             .reset_index())
        age_group_summary['age'] = group
        age_aggregates.append(age_group_summary)
    pop_agg = pd.concat(age_aggregates, ignore_index=True)
    return pop_agg

ages_1 = {
    '15_to_25': ['15_to_20', '20_to_25'],
    '25_to_35': ['25_to_30', '30_to_35'],
    '35_to_45': ['35_to_40', '40_to_45'],
    #'45_to_55': ['45_to_50', '50_to_55'],
    '55_to_65': ['55_to_60', '60_to_65']
}

population_sub = get_pop_agg(population, ages_1)
pop = pd.concat([population[population.age.isin(['0_to_5', '5_to_15', '65_to_125'])], population_sub], ignore_index=True)
pop = pop.set_index(['location', 'age', 'sex', 'year']).sort_index().reset_index()

# all ages except 45 to 55
population_all_ages = (population
                       .loc[population.age.isin(['0_to_5', '5_to_15', '15_to_20', '20_to_25', '25_to_30', '30_to_35', '35_to_40', '40_to_45', '55_to_60', '60_to_65','65_to_125'])]
                       .groupby(['location', 'sex', 'year'])
                       .population.sum()
                       .reset_index())

labels = {
    '15_to_50': ['15_to_25', '25_to_35', '35_to_45'],
    '50+': ['55_to_65', '65_to_125']
}

def aggregate_by_age(data: pd.DataFrame):
    age_aggregates = []
    age_weights = []
    for group, ages in labels.items():
        age_group = data[data.age.isin(ages)]
        age_group_summary = (age_group
                             .groupby(['location', 'year', 'sex'])
                             .population.sum()
                             .reset_index())
        age_group_summary['age'] = group
        age_aggregates.append(age_group_summary)
        age_group_weight = age_group.set_index(['location', 'age', 'sex', 'year']) / \
                           age_group_summary.drop(columns=['age']).set_index(['location', 'sex', 'year'])
        age_group_weight = age_group_weight.reset_index().rename(columns={'population': 'population_weight'})
        age_weights.append(age_group_weight)
    
    pop_agg = pd.concat(age_aggregates, ignore_index=True)
    pop_weight = pd.concat(age_weights, ignore_index=True)
    
    return pop_agg, pop_weight

pop_agg, pop_weight = aggregate_by_age(pop)

# Draw-specific pulmonary TB incidence estimates
l = []
for draw in range(1000):
    df = pd.read_hdf(f'/share/scratch/users/yongqx2/hh_incident_pulmonary_tb_estimates/draw_{draw}.hdf')
    l.append(df)
data = pd.concat(l, ignore_index=True)
# convert value from object to numeric data type
data['value'] = pd.to_numeric(data['value'], errors='coerce')
data['age'] = list(map(lambda x, y: f'{x}_to_{y}', data['age_group_start'], data['age_group_end']))
data['location'] = data.location.replace({'Russian_Federation': 'Russia', 'South_Africa': 'South Africa'})
data['year'] = 2019

# Calculate percent of individuals living in household with newly diagnosed pulmonary TB (95% CI)
def get_age_specific_results(data: pd.DataFrame, pop_weight: pd.DataFrame):
    age_aggregates = []
    for group, ages in labels.items():
        data_sub = data[data.age.isin(ages)]
        pop_weight_sub = pop_weight[pop_weight.age.isin(ages)]
        merged = pd.merge(data_sub[['location', 'age', 'sex', 'year', 'draw', 'value']],
                          pop_weight_sub[['location', 'age', 'sex', 'year', 'population_weight']],
                          how='inner',
                          on=['location', 'age', 'sex', 'year'])
        # population weighted average of pr_pulmonary_tb_in_hh
        merged['weighted_value'] = merged['value'] * merged['population_weight']
        g = (merged
             .groupby(['location', 'sex', 'year', 'draw'])
             .weighted_value.sum()
             .reset_index())
        g['age'] = group
        age_aggregates.append(g)
    results = pd.concat(age_aggregates, ignore_index=True)
    results.rename(columns={'weighted_value': 'value'}, inplace=True)
    return results

data_age_specific = get_age_specific_results(data, pop_weight)
data_final = pd.concat([data.loc[data.age.isin(['0_to_5', '5_to_15']), ['location', 'age', 'sex', 'year', 'draw', 'value']], data_age_specific], ignore_index=True)
data_final = data_final.set_index(['location', 'age', 'sex', 'year', 'draw']).sort_index().reset_index()

population_age_specific = pd.concat([population[population.age.isin(['0_to_5', '5_to_15'])], pop_agg])
age_weight = (population_age_specific
              .set_index(['location', 'age', 'sex', 'year'])
              .div(population_all_ages.set_index(['location', 'sex', 'year']))
              .reset_index())
age_weight.rename(columns={'population': 'weight'}, inplace=True)

def get_all_ages_result(data: pd.DataFrame, pop_weight: pd.DataFrame):
    merged = pd.merge(data, pop_weight, how='inner', on=['location', 'age', 'sex', 'year'])
    merged['weighted_value'] = merged['value'] * merged['weight']
    g = (merged
         .groupby(['location', 'sex', 'year', 'draw'])
         .weighted_value.sum()
         .reset_index())
    g['age'] = 'all_ages'
    g.rename(columns={'weighted_value': 'value'}, inplace=True)
    return g

data_final_all_ages = get_all_ages_result(data_final, age_weight)
data_final_with_all_ages = pd.concat([data_final, data_final_all_ages], ignore_index=True)

def population_sex_agg(pop: pd.DataFrame, pop_all_ages: pd.DataFrame):
    mask = pop_all_ages.copy()
    mask['age'] = 'all_ages'
    population = pd.concat([pop, mask], ignore_index=True)
    g = population.groupby(['location', 'age', 'year']).population.sum()
    sex_weight = (population.set_index(['location', 'age', 'sex', 'year']).population / g).reset_index()
    sex_weight.rename(columns={'population': 'weight'}, inplace=True)
    return sex_weight

pop_sex_weight = population_sex_agg(population_age_specific, population_all_ages)

def aggregate_by_sex(data: pd.DataFrame, sex_weight: pd.DataFrame):
    merged = pd.merge(data, sex_weight, how='inner', on=['location', 'age', 'sex', 'year'])
    merged['weighted_value'] = merged['value'] * merged['weight']
    g = (merged
         .groupby(['location', 'age', 'year', 'draw'])
         .weighted_value.sum()
         .reset_index())
    g['sex'] = 'Both'
    g.rename(columns={'weighted_value': 'value'}, inplace=True)
    summary = (g
               .groupby(['location', 'age', 'sex', 'year'])
               .value.describe(percentiles=[.025, .975])
               .filter(['mean', '2.5%', '97.5%'])
               .reset_index()) 
    return g, summary

data_final_draw, data_final_summary = aggregate_by_sex(data_final_with_all_ages, pop_sex_weight)

def group_pop(pop: pd.DataFrame):
    pop_all_ages = pop.groupby(['location', 'sex', 'year']).population.sum().reset_index()
    pop_all_ages['age'] = 'all_ages'
    pop_with_all_ages = pd.concat([pop, pop_all_ages])
    pop_all_sexes = pop_with_all_ages.groupby(['location', 'age', 'year']).population.sum().reset_index()
    pop_all_sexes['sex'] = 'Both'
    return pop_all_sexes

population_agg = group_pop(population)

ages_2 = {
    '15_to_50': ['15_to_20', '20_to_25', '25_to_30', '30_to_35', '35_to_40', '40_to_45', '45_to_50'],
    '50+': ['50_to_55', '55_to_60', '60_to_65', '65_to_125']
}

population_final = pd.concat([population_agg[population_agg.age.isin(['0_to_5', '5_to_15', 'all_ages'])], get_pop_agg(population_agg, ages_2)])
population_final = population_final.set_index(['location', 'age', 'sex', 'year']).sort_index().reset_index()

def get_hhc_count(data: pd.DataFrame, population: pd.DataFrame):
    merged = pd.merge(data, population, how='inner', on=['location', 'age', 'sex', 'year'])
    merged['count_value'] = merged['value'] * merged['population']
    count = merged[['location', 'age', 'sex', 'year', 'draw', 'count_value']]
    count_all_countries = count.groupby(['age', 'sex', 'year', 'draw']).count_value.sum().reset_index()
    count_all_countries['location'] = 'All countries'
    count_with_all_countries = pd.concat([count, count_all_countries], ignore_index=True)
    count_summary = (count_with_all_countries
                     .groupby(['location', 'age', 'sex', 'year'])
                     .count_value.describe(percentiles=[.025, .975])
                     .filter(['mean', '2.5%', '97.5%'])
                     .reset_index())
    return count_with_all_countries, count_summary

count_draw, count = get_hhc_count(data_final_draw, population_final)

# Table 1
mask = count.copy()
mask[['mean', '2.5%', '97.5%']] = round(mask[['mean', '2.5%', '97.5%']]/10000, 1)
mask['value'] = list(map(lambda x, y, z: f'{x} ({y} - {z})',
                         mask['mean'], mask['2.5%'], mask['97.5%']))
mask = mask.loc[:, ['location', 'age', 'sex', 'year', 'value']]
mask['age_rank'] = mask.age.map({'0_to_5': 1, '5_to_15': 2, '15_to_50': 3, '50+': 4, 'all_ages': 5})
mask = mask.set_index(['location', 'age_rank']).sort_index().reset_index()
mask.drop(columns='age_rank', inplace=True)
mask.to_csv('/home/j/Project/simulation_science//latent_tuberculosis_infection/hhc_result/20210816_count_of_NDPTB_hhc_formatted.csv', index=False)

# Figrue 2
def plot_percent(data: pd.DataFrame, age_to_rank: str):
    data = data.copy()
    data['location'] = data.location.replace({'Congo': 'DR Congo'})
    data[['mean', '2.5%', '97.5%']] *= 100
    # rank locations by percent_mean of all ages 
    locations = list(data
                     .loc[data.age == age_to_rank]
                     .sort_values(by='mean', ascending=False)
                     .location)
    age_groups = {'0_to_5': '0 to 4', '5_to_15': '5 to 14', '15_to_50': '15 to 49', '50+': '50+'}
    
    plt.figure(figsize=(18, 9), dpi=300)
    xx = np.arange(len(locations))
    width = .2

    for i, location in enumerate(locations):
        data_location = data.loc[data.location == location]
        for j, age_group in enumerate(age_groups.keys()):
            data_age = data_location.loc[data_location.age == age_group]
            yy = data_age['mean']
            ll = data_age['mean'] - data_age['2.5%']
            uu = data_age['97.5%'] - data_age['mean']

            plt.bar(i+width*j, yy, width, yerr=[ll, uu], color=f'C{j}', label=age_groups[age_group])
        
        plt.xticks(xx+1.5*width, locations, rotation=60, fontsize=12)
        plt.legend(loc='best', fontsize=12) if i == 0 else None
        plt.title('Individuals in household with exposure to newly diagnosed active pulmonary TB', fontsize=16)
        plt.ylabel('Percent (%)', fontsize=14)
    plt.savefig(f'/home/j/Project/simulation_science/latent_tuberculosis_infection/hhc_result/hh_NDPTB_exposure_rank_by_{age_to_rank}.pdf', bbox_inches='tight')
    plt.savefig(f'/home/j/Project/simulation_science/latent_tuberculosis_infection/hhc_result/hh_NDPTB_exposure_rank_by_{age_to_rank}.png', bbox_inches='tight')

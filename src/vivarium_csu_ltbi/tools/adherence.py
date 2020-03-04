import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def load_data(path: str, model_version: str, file_name: str):
    output = []
    locations = ['ethiopia', 'india', 'peru', 'philippines', 'south_africa']
    for location in locations:
        master_dir = path + f'{model_version}_{location}_model_results/'
        sub_dir = master_dir + os.listdir(master_dir)[0]
        assert file_name in os.listdir(sub_dir), f'No such a file in {location}'
        df = pd.read_csv(sub_dir + '/' + file_name).iloc[:, 1:]
        df['location'] = location
        output.append(df)
    return pd.concat(output)


def aggregate_treatment_groups(data: pd.DataFrame):
    labels = {'all': ['untreated', '6H_adherent', '6H_nonadherent', '3HP_adherent', '3HP_nonadherent'],
              'treated': ['6H_adherent', '6H_nonadherent', '3HP_adherent', '3HP_nonadherent'],
              'adherent':['6H_adherent', '3HP_adherent']}
    treatment_aggregates = []
    for group, treatments in labels.items():
        treatment_group = data[data.treatment_group.isin(treatments)]
        treatment_group = (treatment_group
                           .drop(columns=['treatment_group'])
                           .groupby(['location', 'scenario', 'risk_group', 'cause', 'age', 'sex', 'year', 'draw'])
                           .sum()
                           .reset_index())
        treatment_group['treatment_group'] = group
        treatment_aggregates.append(treatment_group)
    
    return pd.concat(treatment_aggregates, ignore_index=True)


def calc_adherent_prop(person_time: pd.DataFrame)
    idx_cols = ['location', 'scenario', 'risk_group', 'cause', 'age', 'sex', 'year', 'draw']

    adherent = person_time[person_time.treatment_group == 'adherent'].drop(columns='treatment_group')
    treated = person_time[person_time.treatment_group == 'treated'].drop(columns='treatment_group')
    all_tx = person_time[person_time.treatment_group == 'all'].drop(columns='treatment_group')
    
    prop_treated = (adherent.set_index(idx_cols) / treated.set_index(idx_cols)).reset_index()
    prop_all_tx = (adherent.set_index(idx_cols) / all_tx.set_index(idx_cols)).reset_index()

    summary_treated = (prop_treated
                       .groupby(idx_cols[:-1])
                       .value
                       .describe(percentiles=[.025, .975])
                       .filter(['mean', '2.5%', '97.5%'])
                       .reset_index())
    summary_treated['population_subgroup'] = 'treated'
    
    summary_all_tx = (prop_all_tx
                      .groupby(idx_cols[:-1])
                      .value
                      .describe(percentiles=[.025, .975])
                      .filter(['mean', '2.5%', '97.5%'])
                      .reset_index())
    summary_all_tx['population_subgroup'] = 'all treatment'
    
    return summary_treated, summary_all_tx


def plot_adherent_prop_by_year(data: pd.DataFrame, location: str, risk_group: str):
    df = data.copy()
    df[['mean', '2.5%', '97.5%']] *= 100
    locations = {
        'ethiopia': 'Ethiopia', 'india': 'India', 'peru': 'Peru', 'philippines': 'Philippines', 'south_africa': 'South Africa'
    }
    risk_groups = {'all_population': 'All Population', 'plwhiv': 'PLHIV', 'u5_hhtb': 'U5 HHC'}
    scenarios = {
        '3HP_scale_up': 'Intervention 2 (3HP scale up)',
        '6H_scale_up': 'Intervention 1 (6H scale up)',
        'baseline': 'Baseline (6H as projected)'
    }
    df['location'] = df.location.map(locations)
    df['risk_group'] = df.risk_group.map(risk_groups)
    df['scenario'] = df.scenario.map(scenarios)
    
    population_subgroup = df.population_subgroup.unique()[0]
    age = 'all' if risk_group == 'PLHIV' else '0_to_5'

    df = df.loc[(df.location == location)
                & (df.year != 'all')
                & (df.sex == 'all')
                & (df.age == age)
                & (df.risk_group == risk_group)]
    
    fig = plt.figure(figsize=(8, 4), dpi=150)
    
    for scenario in ['Intervention 2 (3HP scale up)', 'Intervention 1 (6H scale up)', 'Baseline (6H as projected)']:
        t = df.loc[df.scenario == scenario]
        xx = t['year']
        yy = t['mean']
        lb = t['2.5%']
        ub = t['97.5%']    

        plt.plot(xx, yy, '-o', label=scenario)
        plt.fill_between(xx, lb, ub, alpha=.2)

    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Treatment Adherence\n (percentage points)', fontsize=12)
    plt.title(f'{location}, Effective Coverage Among {risk_group}', fontsize=12)
    plt.legend(loc=(1.05, .05))
    plt.grid()
    
    master_dir = '/home/j/Project/simulation_science/latent_tuberculosis_infection/result/final_results_plot/'
    fig.savefig(master_dir + f'{location}/{risk_group} adherence among {population_subgroup} group by year.png',
                bbox_inches='tight')


if __name__ = '__main__':
    path = '/home/j/Project/simulation_science/latent_tuberculosis_infection/result/make_results/'
    model_version = 'no_3hp_babies_10_no_3hp_babies_100'
    file_name = 'person_time_count_data.csv'

    person_time = load_data(path, model_version, file_name)
    person_time = aggregate_treatment_groups(person_time)
    summary_treated, summary_all_tx = calc_adherent_prop(person_time)
    df = pd.concat([summary_treated, summary_all_tx], ignore_index=True)
    df.to_csv(path + 'adherence_proportion.csv', index=False)

    for location in ['Ethiopia', 'India', 'Peru', 'Philippines', 'South Africa']:
        for risk_group in ['PLHIV', 'U5 HHC']:
            plot_adherent_prop_by_year(summary_treated, location, risk_group)
            plot_adherent_prop_by_year(summary_all_tx, location, risk_group)
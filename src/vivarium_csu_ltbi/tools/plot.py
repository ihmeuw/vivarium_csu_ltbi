import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import os
import datetime
import warnings
warnings.filterwarnings('ignore')

master_dir = '/home/j/Project/simulation_science/latent_tuberculosis_infection/result/final_results_plot/'

location_names = ['Ethiopia', 'India', 'Peru', 'Philippines', 'South Africa']
age_groups = ['0 to 4', '5 to 14', '15 to 59', '60 plus']
scenarios = ['Intervention 2 (3HP scale up)', 'Intervention 1 (6H scale up)', 'Baseline (6H as projected)']
outcomes = [
    'Active TB Incidence count (cases)',
    'Active TB Incidence rate (cases per 100,000 person-years)',
    'Deaths due to Active TB (per 100,000 person-years)',
    'Deaths due to HIV resulting in other diseases (per 100,000 person-years)',
    'DALYs due to Active TB (per 100,000 person-years)',
    'DALYs due to HIV resulting in other diseases (per 100,000 person-years)',
]

def load_data(path: str, model_version: str):
    output = []
    locations = ['ethiopia', 'india', 'peru', 'philippines', 'south_africa']
    for location in locations:
        master_dir = path + f'{model_version}_{location}_model_results/'
        sub_dir = master_dir + os.listdir(master_dir)[0]
        f = 'aggregate_final_table.csv'
        assert f in os.listdir(sub_dir), f'No such a file in {location}'
        df = pd.read_csv(sub_dir + '/' + f)
        output.append(df)
    return pd.concat(output, ignore_index=True)

def format_data(df: pd.DataFrame):
    outcomes = {
        'treatment_coverage': 'Treatment Coverage (proportion)',
        'actb_incidence_count': 'Active TB Incidence count (cases)',
        'actb_incidence_rate': 'Active TB Incidence rate (cases per 100,000 person-years)',
        'deaths_due_to_all_form_tb': 'Deaths due to Active TB (per 100,000 person-years)',
        'deaths_due_to_hiv_other': 'Deaths due to HIV resulting in other diseases (per 100,000 person-years)',
        'deaths_due_to_other_causes': 'Deaths due to other causes (per 100,000 person-years)',
        'dalys_due_to_all_form_tb': 'DALYs due to Active TB (per 100,000 person-years)',
        'dalys_due_to_hiv_other': 'DALYs due to HIV resulting in other diseases (per 100,000 person-years)',
        'dalys_due_to_other_causes': 'Ylls due to other causes (per 100,000 person-years)',
        'person_time': 'Person-Years'
    }
    locations = {'ethiopia': 'Ethiopia', 'india': 'India', 'peru': 'Peru', 'philippines': 'Philippines', 'south_africa': 'South Africa'}
    age_groups = {'0_to_5': '0 to 4', '5_to_15': '5 to 14', '15_to_60': '15 to 59', '60+': '60 plus', 'all': 'All Ages'}
    sexes = {'all': 'Both', 'female': 'Female', 'male': 'Male'}
    risk_groups = {'all_population': 'All Population', 'plwhiv': 'PLHIV', 'u5_hhtb': 'U5 HHC'}
    scenarios = {'3HP_scale_up': 'Intervention 2 (3HP scale up)', '6H_scale_up': 'Intervention 1 (6H scale up)', 'baseline': 'Baseline (6H as projected)'}
    treatment_groups = {
        '3HP_adherent': '3HP adherent', '3HP_nonadherent': '3HP non-adherent',
        '6H_adherent': '6H adherent', '6H_nonadherent': '6H non-adherent',
        'untreated': 'Untreated', 'all': 'All'
    }
    
    df['outcome'] = df.outcome.map(outcomes)
    df['location'] = df.location.map(locations)
    df['year'] = df.year.replace('all', 'All Years')
    df['age'] = df.age.map(age_groups)
    df['sex'] = df.sex.map(sexes)
    df['risk_group'] = df.risk_group.map(risk_groups)
    df['scenario'] = df.scenario.map(scenarios)
    df['treatment_group'] = df.treatment_group.map(treatment_groups)
    
    t = df.rename(columns={'outcome': 'Outcome', 'location': 'Location name',
                           'year': 'Year', 'age': 'Age group', 'sex': 'Sex',
                           'risk_group': 'Risk group', 'scenario': 'Scenario', 'treatment_group': 'Treatment group',
                           'mean': 'Mean value', 'ub': 'Upper uncertainty interval', 'lb': 'Lower uncertainty interval',
                           'averted_mean': 'Difference from baseline - mean value',
                           'averted_ub': 'Difference from baseline - upper uncertainty interval',
                           'averted_lb': 'Difference from baseline - lower uncertainty interval'})
    return df, t

def plot_outcome_by_year(df, location, risk_group, outcome, outcome_type):
    age = 'All Ages' if risk_group == 'PLHIV' else '0 to 4'

    t = df.loc[(df['location'] == location)
               & (df['sex'] == 'Both')
               & (df['age'] == age)
               & (df['risk_group'] == risk_group)
               & (df['outcome'] == outcome)]
    t = t.loc[t['year'] != 'All Years']
    
    fig = plt.figure(figsize=(10, 6), dpi=150)
    
    outcome_name = outcome.split(' (')[0]
    if outcome == 'Active TB Incidence count (cases)':
        outcome_metric = 'cases'
    elif outcome == 'Active TB Incidence rate (cases per 100,000 person-years)':
        outcome_metric = f'cases per 100,000 {risk_group} person-years'
    else:
        outcome_metric = f'per 100,000 {risk_group} person-years'

    if outcome_type == 'value':
        scenarios = ['Intervention 2 (3HP scale up)', 'Intervention 1 (6H scale up)', 'Baseline (6H as projected)']
        prefix = ''
        title = f'{location}, {risk_group}, {outcome_name} by Year'
    else:
        scenarios = ['Intervention 2 (3HP scale up)', 'Intervention 1 (6H scale up)']
        prefix = 'Averted'
        title = f'{location}, {risk_group}, Averted {outcome_name} by Year, Compared to Baseline'

    for scenario in scenarios:
        t_scenario = t.loc[t['scenario'] == scenario]
        xx = t_scenario['year']
        if outcome_type == 'value':
            yy = t_scenario['mean']
            lb = t_scenario['lb']
            ub = t_scenario['ub']
        else:
            yy = t_scenario['averted_mean']
            lb = t_scenario['averted_lb']
            ub = t_scenario['averted_ub']
        
        plt.plot(xx, yy, '-o', label=scenario)
        plt.fill_between(xx, lb, ub, alpha=.2)
        
    plt.xlabel('Year', fontsize=12)
    plt.ylabel(f'{prefix} {outcome_name}\n({outcome_metric})', fontsize=12)
    plt.title(title, fontsize=12)
    plt.legend(loc=(1.05, 0.05))
    plt.grid()
    fig.savefig(master_dir + f'{location}/' + \
                f'{risk_group} {outcome_name} {outcome_type} by year.png',
                bbox_inches='tight')

def plot_averted_by_age(df, location, outcome):
    t = df.loc[(df['location'] == location)
               & (df['year'] == 'All Years')
               & (df['sex'] == 'Both')
               & (df['risk_group'] == 'PLHIV')
               & (df['outcome'] == outcome)]
    t = t.loc[t['age'] != 'All Ages']
    
    fig = plt.figure(figsize=(10, 6), dpi=150)
    xx = np.arange(4)
    width = .4 
    
    s_3hp = t.loc[t['scenario'] == 'Intervention 2 (3HP scale up)']
    s_6h = t.loc[t['scenario'] == 'Intervention 1 (6H scale up)']

    yy_3hp = s_3hp['averted_mean']
    ll_3hp = yy_3hp - s_3hp['averted_lb']
    uu_3hp = s_3hp['averted_ub'] - yy_3hp

    yy_6h = s_6h['averted_mean']
    ll_6h = yy_6h - s_6h['averted_lb']
    uu_6h = s_6h['averted_ub'] - yy_6h
        
    plt.bar(xx, yy_3hp, width, yerr=[ll_3hp, uu_3hp], label='Intervention 2 (3HP scale up)')
    plt.bar(xx+width, yy_6h, width, yerr=[ll_6h, uu_6h], label='Intervention 1 (6H scale up)')
    
    outcome_name = outcome.split(' (')[0]
    if outcome == 'Active TB Incidence count (cases)':
        outcome_metric = 'cases'
    elif outcome == 'Active TB Incidence rate (cases per 100,000 person-years)':
        outcome_metric = 'cases per 100,000 PLHIV person-years'
    else:
        outcome_metric = 'per 100,000 PLHIV person-years'
    
    plt.xticks(xx+width/2, age_groups)
    plt.xlabel('Age Group', fontsize=12)
    plt.ylabel(f'Averted {outcome_name}\n({outcome_metric})', fontsize=12)
    plt.title(f'{location}, PLHIV, Averted {outcome_name} by Age, Compared to Baseline', fontsize=12)
    plt.legend(loc=(1.05, 0.05))
    plt.grid(axis='y', alpha=.5)
    fig.savefig(master_dir + f'{location}/' + \
                f'PLHIV {outcome_name} averted by age.png',
                bbox_inches='tight')

def plot_person_time_by_year(df, age_group, risk_group, location):
    df = df.loc[(df.outcome == 'Person-Years') & (df.location == location)].copy()
    df[['mean', 'lb', 'ub']] = df[['mean', 'lb', 'ub']]/1000
    df = df.loc[(df.year != 'All Years')
                & (df.sex == 'Both')
                & (df.age == age_group)
                & (df.risk_group == risk_group)]
    
    fig = plt.figure(figsize=(8, 4), dpi=150)
    
    for scenario in scenarios:
        t = df.loc[df.scenario == scenario]
        
        xx = t['year']
        yy = t['mean']
        lb = t['lb']
        ub = t['ub']    

        plt.plot(xx, yy, '-o', label=scenario)
        plt.fill_between(xx, lb, ub, alpha=.2)
    
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Population in thousands', fontsize=12)
    plt.title(f'{location}, {risk_group}, Population by Year', fontsize=12)
    plt.legend(loc=(1.05, .05))
    plt.grid()
    fig.savefig(master_dir + f'{location}/' + \
                f'{risk_group} population by year.png',
                bbox_inches='tight')

def compare_across_countries(df, location_names, outcome, risk_group, scenario):
    age = 'All Ages' if risk_group == 'PLHIV' else '0 to 4'
    
    t = df.loc[(df.year != 'All Years')
               & (df.sex == 'Both')
               & (df.outcome == outcome)
               & (df.age == age)
               & (df.risk_group == risk_group)
               & (df.scenario == scenario)]
    
    fig = plt.figure(figsize=(10, 6), dpi=150)
    
    for location in location_names:
        t_location = t.loc[t.location == location]
        xx = t_location['year']
        yy = t_location['averted_mean']
        lb = t_location['averted_lb']
        ub = t_location['averted_ub']

        plt.plot(xx, yy, '-o', label=location)
        # plt.fill_between(xx, lb, ub, alpha=.2)

    outcome_name = outcome.split(' (')[0]
    if outcome == 'Active TB Incidence count (cases)':
        outcome_metric = 'cases'
    elif outcome == 'Active TB Incidence rate (cases per 100,000 person-years)':
        outcome_metric = f'cases per 100,000 {risk_group} person-years'
    else:
        outcome_metric = f'per 100,000 {risk_group} person-years'
    
    plt.xlabel('Year', fontsize=12)
    plt.ylabel(f'Averted {outcome_name}\n({outcome_metric})', fontsize=12)
    plt.title(f'{scenario} compared to Baseline, {risk_group}, Averted {outcome_name} by Year', fontsize=12)
    plt.legend(loc=(1.05, 0.05))
    plt.grid()
    fig.savefig(master_dir + f'{scenario} {risk_group} {outcome_name} averted by year.png',
                bbox_inches='tight')

def plot_coverage(df, location, risk_group):
    age = 'All Ages' if risk_group == 'PLHIV' else '0 to 4'
    
    t = df.loc[(df.outcome == 'Treatment Coverage (proportion)') 
                & (df.location == location) 
                & (df.year != 'All Years') 
                & (df.age == age) 
                & (df.sex == 'Both') 
                & (df.risk_group == risk_group)]
    t.rename(columns={'year': 'Year', 'mean': 'Coverage (proportion)'}, inplace=True)

    sns.set(font_scale=3)
    g = sns.factorplot(x='Year', y='Coverage (proportion)', hue='treatment_group',
                       col='scenario', size=10, aspect=1,
                       sharey=True, data=t)
    
    g.fig.suptitle(f'{location}, {risk_group}')
    g._legend.set_title('Treatment Group')
    plt.subplots_adjust(wspace=0.1, top=0.85)
    g.savefig(master_dir + f'{location}/' + \
              f'{risk_group} coverage.png',
              bbox_inches='tight')

if __name__ == '__main__':
    result_dir = '/home/j/Project/simulation_science/latent_tuberculosis_infection/result/make_results/'
    model_version = 'no_3hp_babies_10_no_3hp_babies_100'
    time = ''.join(str(datetime.date.today()).split('-'))
    age_end = 'merged_ages'
    df = load_data(result_dir, model_version)
    df, t = format_data(df)
    t.to_csv(result_dir + f'{time}_ltbi_final_results_{age_end}_formatted.csv', index=False)
    # make plot for outcome by year
    for location in location_names:
        for risk_group in ['PLHIV', 'U5 HHC']:
            for outcome in outcomes:
                for outcome_type in ['value', 'averted']:
                    plot_outcome_by_year(df, location, risk_group, outcome, outcome_type)
    # make plot for averted outcome by age
    for location in location_names:
        for outcome in outcomes:
            plot_averted_by_age(df, location, outcome)
    # make plot for population by year
    for location in location_names:
        plot_person_time_by_year(df, 'All Ages', 'PLHIV', location)
        plot_person_time_by_year(df, '0 to 4', 'U5 HHC', location)
    # make plot for comparison across countries
    for outcome in outcomes:
        for risk_group in ['PLHIV', 'U5 HHC']:
            for scenario in ['Intervention 2 (3HP scale up)', 'Intervention 1 (6H scale up)']:
                compare_across_countries(df, location_names, outcome, risk_group, scenario)
    # make plot for coverage by year
    for location in location_names:
        plot_coverage(df, location, 'PLHIV')
        plot_coverage(df, location, 'U5 HHC')

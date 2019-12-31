import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

master_dir = '/home/j/Project/simulation_science/ \
             latent_tuberculosis_infection/result/interim_results_plot/'

location_names = ['ethiopia', 'india', 'peru', 'philippines', 'south_africa']
age_groups = ['all', '0_to_5', '5_to_15', '15_to_60', '60+']
# disease burden
outcomes = ['actb_incidence_count', 'actb_incidence_rate',
            'deaths_due_to_all_form_tb', 'dalys_due_to_all_form_tb',
            'deaths_due_to_hiv_other', 'dalys_due_to_hiv_other',
            'deaths_due_to_other_causes', 'dalys_due_to_other_causes']

def plot_outcome_by_year(df, location, risk_group, outcome, outcome_type):
    age = 'all' if risk_group == 'plwhiv' else '0_to_5'

    t = df.loc[(df['location'] == location)
               & (df['sex'] == 'all')
               & (df['age'] == age)
               & (df['risk_group'] == risk_group)
               & (df['outcome'] == outcome)]
    t = t.loc[t['year'] != 'all']
    
    plt.figure(figsize=(10, 6), dpi=150)
    
    if outcome_type == 'value':
        scenarios = ['3HP_scale_up', '6H_scale_up', 'baseline']
    else:
        scenarios = ['3HP_scale_up', '6H_scale_up']
    
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
    
    prefix = 'Averted' if outcome_type == 'averted' else ''
    if outcome == 'actb_incidence_count':
        unit = 'cases'
    elif outcome == 'actb_incidence_rate':
        unit = 'cases per 100,000 PY'
    else:
        unit = 'per 100,000 PY'
    if location == 'south_africa':
        loc_name = 'South Africa'  
    else:
        loc_name = location.capitalize()
    plt.xlabel('Year', fontsize=16)
    plt.ylabel(f'{prefix} {outcome} \n({unit})', fontsize=16)
    plt.title(f'{loc_name}, {risk_group.upper()}, {prefix} {outcome} by year', fontsize=16)
    plt.legend(loc=(1.05, 0.05))
    plt.grid()
    plt.savefig(master_dir + f'{location}/' + \
                f'{location}_{risk_group}_{outcome}_{outcome_type}_by_year.png',
                bbox_inches='tight')

def plot_averted_by_age(df, location, outcome):
    t = df.loc[(df['location'] == location)
               & (df['year'] == 'all')
               & (df['sex'] == 'all')
               & (df['risk_group'] == 'plwhiv')
               & (df['outcome'] == outcome)]
    t = t.loc[t['age'] != 'all']
    
    plt.figure(figsize=(10, 6), dpi=150)
    xx = np.arange(4)
    width = .4 
    
    s_3hp = t.loc[t['scenario'] == '3HP_scale_up']
    s_6h = t.loc[t['scenario'] == '6H_scale_up']

    yy_3hp = s_3hp['averted_mean']
    ll_3hp = yy_3hp - s_3hp['averted_lb']
    uu_3hp = s_3hp['averted_ub'] - yy_3hp

    yy_6h = s_6h['averted_mean']
    ll_6h = yy_6h - s_6h['averted_lb']
    uu_6h = s_6h['averted_ub'] - yy_6h
        
    plt.bar(xx, yy_3hp, width, yerr=[ll_3hp, uu_3hp], label='3HP_scale_up')
    plt.bar(xx+width, yy_6h, width, yerr=[ll_6h, uu_6h], label='6H_scale_up')
       
    plt.xticks(xx+width/2, age_groups[1:])
    
    if outcome == 'actb_incidence_count':
        unit = 'cases'
    elif outcome == 'actb_incidence_rate':
        unit = 'cases per 100,000 PY'
    else:
        unit = 'per 100,000 PY'
    if location == 'south_africa':
        loc_name = 'South Africa'  
    else:
        loc_name = location.capitalize()
    
    plt.xlabel('Age Group', fontsize=16)
    plt.ylabel(f'Averted {outcome} \n({unit})', fontsize=16)
    plt.title(f'{loc_name}, PLWHIV, Averted {outcome} by age', fontsize=16)
    plt.legend(loc=(1.05, 0.05))
    plt.grid(axis='y', alpha=.5)
    plt.savefig(master_dir + f'{location}/' + \
                f'{location}_plwhiv_{outcome}_averted_by_age.png',
                bbox_inches='tight')

def plot_person_time_by_year(df, age_group, risk_group, location):
    df = df.loc[(df.outcome == 'person_time') & (df.location == location)].copy()
    df[['mean', 'lb', 'ub']] = df[['mean', 'lb', 'ub']]/1000
    df = df.loc[df.year != 'all']
    df = df.loc[(df.sex == 'all')
                & (df.age == age_group)
                & (df.risk_group == risk_group)]
    
    plt.figure(figsize=(8, 4), dpi=150)
    
    for scenario in scenarios:
        t = df.loc[df.scenario == scenario]
        
        xx = t['year']
        yy = t['mean']
        lb = t['lb']
        ub = t['ub']    

        plt.plot(xx, yy, '-o', label=scenario)
        plt.fill_between(xx, lb, ub, alpha=.2)
    
    plt.xlabel('Year', fontsize=14)
    plt.ylabel('Population in thousands', fontsize=14)
    if location != 'south_africa':
        plt.title(f'{location.capitalize()}, {risk_group.upper()}, population by year', fontsize=14)
    else:
        plt.title(f'South Africa, {risk_group.upper()}, population by year', fontsize=14)
    plt.legend(loc=(1.05, .05))
    plt.grid()
    plt.savefig(master_dir + f'{location}/' + \
                f'{location}_{risk_group}_population_by_year.png',
                bbox_inches='tight')

def compare_across_countries(df, location_names, outcome, age, risk_group, scenario):
    df = df[(df.year != 'all') & (df.sex == 'all')]
    
    t = df[(df.outcome == outcome)
           & (df.age == age)
           & (df.risk_group == risk_group)
           & (df.scenario == scenario)]
    
    plt.figure(figsize=(10, 6), dpi=150)
    
    for location in location_names:
        if location == 'south_africa':
            loc_name = 'South Africa'  
        else:
            loc_name = location.capitalize()
        
        t_location = t[t.location == location]
        xx = t_location['year']
        yy = t_location['averted_mean']
        lb = t_location['averted_lb']
        ub = t_location['averted_ub']

        plt.plot(xx, yy, '-o', label=loc_name)
        plt.fill_between(xx, lb, ub, alpha=.2)

    if outcome == 'actb_incidence_count':
        unit = 'cases'
    elif outcome == 'actb_incidence_rate':
        unit = 'cases per 100,000 PY'
    else:
        unit = 'per 100,000 PY'


    plt.xlabel('Year', fontsize=16)
    plt.ylabel(f'Averted {outcome} \n({unit})', fontsize=16)
    plt.title(f'{scenario}, {risk_group.upper()}, Averted {outcome} by year', fontsize=16)
    plt.legend(loc=(1.05, 0.05))
    plt.grid()
    plt.savefig(master_dir + \
                f'{scenario}_{risk_group}_{outcome}_averted_by_year.png',
                bbox_inches='tight')

def percent_averted(df, location, age, risk_group, outcome='actb_incidence_rate'):
    df = df[(df.year != 'all') & (df.sex == 'all')]
    
    t = df[(df.location == location)
           & (df.outcome == outcome)
           & (df.age == age)
           & (df.risk_group == risk_group)]
    #calculate percent averted
    val_cols = ['mean', 'ub', 'lb']
    averted_cols = ['averted_mean', 'averted_ub', 'averted_lb']
    numerator = (t.loc[t.scenario != 'baseline', [c for c in t.columns if c not in val_cols]]
                  .rename(columns={'averted_mean': 'mean', 'averted_ub': 'ub', 'averted_lb': 'lb'}))
    denominator = (t.loc[t.scenario == 'baseline', [c for c in t.columns if c not in averted_cols]]
                    .drop(columns='scenario'))
    
    idx_cols = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group']
    p_averted = (numerator.set_index(idx_cols + ['scenario'])[val_cols] /\
                 denominator.set_index(idx_cols)[val_cols]).reset_index()
    p_averted[val_cols] *= 100
    
    #make plot
    plt.figure(figsize=(10, 6), dpi=150)
    for scenario in ['3HP_scale_up', '6H_scale_up']:
        p_averted_scenario = p_averted[p_averted.scenario == scenario]

        xx = p_averted_scenario['year']
        yy = p_averted_scenario['mean']
        lb = p_averted_scenario['lb']
        ub = p_averted_scenario['ub']

        plt.plot(xx, yy, '-o', label=scenario)
        plt.fill_between(xx, lb, ub, alpha=.2)
    
    if location == 'south_africa':
        loc_name = 'South Africa'  
    else:
        loc_name = location.capitalize()
    plt.xlabel('Year', fontsize=16)
    plt.ylabel(f'%Averted {outcome}\n(percentage points)', fontsize=16)
    plt.title(f'{loc_name}, {risk_group.upper()}, %Averted {outcome} by year', fontsize=16)
    plt.legend(loc=(1.05, 0.05))
    plt.grid()
    plt.savefig(master_dir + f'{location}/' + \
                f'{location}_{risk_group}_{outcome}_percent_averted_by_year.png',
                bbox_inches='tight')

def plot_coverage(df, location, risk_group):
    age = 'all' if risk_group == 'plwhiv' else '0_to_5'
    
    t = df.loc[(df.outcome == 'treatment_coverage') 
                & (df.location == location) 
                & (df.year != 'all') 
                & (df.age == age) 
                & (df.sex == 'all') 
                & (df.risk_group == risk_group)]
    t.rename(columns={'year': 'Year', 'mean': 'Coverage'}, inplace=True)

    sns.set(font_scale=3)
    g = sns.factorplot(x='Year', y='Coverage', hue='treatment_group',
                       col='scenario', size=10, aspect=1,
                       sharey=True, data=t)
    
    if location == 'south_africa':
        loc_name = 'South Africa'  
    else:
        loc_name = location.capitalize()
    g.fig.suptitle(f'{loc_name}, {risk_group.upper()}')
    plt.subplots_adjust(wspace=0.1, top=0.85)
    plt.savefig(master_dir + f'{location}/' + \
                f'{location}_{risk_group}_coverage.png',
                bbox_inches='tight')

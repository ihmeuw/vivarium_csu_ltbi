import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

from typing import NamedTuple

output_dir = '/home/j/Project/simulation_science/latent_tuberculosis_infection/result/'

def make_raw_aggregates(data):
    labels = {'0_to_5': ['early_neonatal', 'late_neonatal', 'post_neonatal', '1_to_4'],
              '5_to_15': ['5_to_9', '10_to_14'],
              '15_to_60':['15_to_19', '20_to_24', '25_to_29',
                          '30_to_34', '35_to_39', '40_to_44', 
                          '45_to_49', '50_to_54', '55_to_59',],
              '60+': ['60_to_64', '65_to_69', '70_to_74', '75_to_79',
                      '80_to_84', '85_to_89', '90_to_94', '95_plus']}
    age_aggregates = []
    for group, ages in labels.items():
        age_group = data[data.age.isin(ages)]
        age_group = (age_group
                     .drop(columns=['age'])
                     .groupby(['draw', 'scenario', 'treatment_group', 'hhtb', 'sex', 'year', 'measure'])
                     .sum()
                     .reset_index())
        age_group['age'] = group
        age_aggregates.append(age_group)
    data = pd.concat(age_aggregates)
    
    all_ages = (data
                .groupby(['draw', 'scenario', 'treatment_group', 'hhtb', 'sex', 'year', 'measure'])
                .value.sum()
                .reset_index())
    all_ages['age'] = 'all'
    data = pd.concat([data, all_ages])
    
    both_sexes = (data
                  .groupby(['draw', 'scenario', 'treatment_group', 'hhtb', 'age', 'year', 'measure'])
                  .value.sum()
                  .reset_index())
    both_sexes['sex'] = 'all'
    data = pd.concat([data, both_sexes])
    
    all_years = (data
                 .groupby(['draw', 'scenario', 'treatment_group', 'hhtb', 'sex', 'age', 'measure'])
                 .value.sum()
                 .reset_index())
    all_years['year'] = 'all'
    data = pd.concat([data, all_years])
    
    return data


class MeasureData(NamedTuple):
    deaths: pd.DataFrame
    person_time: pd.DataFrame
    ltbi_person_time: pd.DataFrame
    ylls: pd.DataFrame
    ylds: pd.DataFrame
    tb_cases: pd.DataFrame
    location: str
    
    
def split_measures(data, location):
    deaths = get_measure(data, 'death')
    person_time, ltbi_person_time = get_person_time(data)
    ylls = get_measure(data, 'ylls')
    ylds = get_measure(data, 'ylds')
    tb_cases = get_tb_events(data)
    return MeasureData(deaths, person_time, ltbi_person_time, ylls, ylds, tb_cases, location)
    

def get_measure(data, measure):
    assert measure in ['death', 'ylls', 'ylds']
    data = data[data.measure.str.contains(measure)]
    data['cause'] = data.measure.str.split('due_to_').str[1]
    data = data.drop(columns=['measure'])
    
    data = aggregate_risk_groups(data)
    data = aggregate_measure_by_cause(data)
    return sort_data(data)


def get_person_time(data):
    person_time = data[data.measure.str.contains('_person_time')]
    person_time['cause'] = person_time.measure.str.split('_person_time').str[0]
    person_time = person_time.drop(columns='measure')
    
    data = aggregate_risk_groups(person_time)
    all_person_time, ltbi_person_time = split_and_aggregate_person_time(data)    
    return sort_data(all_person_time), sort_data(ltbi_person_time)


def get_tb_events(data):
    event_counts = data[data.measure.str.contains('event_count')]
    event_counts['from'] = event_counts.measure.str.split('_to_').str[0]
    event_counts['to'] = event_counts.measure.str.split('_to_').str[1].str.split('_event_count').str[0]
    event_counts = event_counts.drop(columns='measure')
    
    tb_events = (event_counts[(event_counts['from'].str.contains('ltbi')) 
                             & (event_counts['to'].str.contains('activetb'))])
    tb_events['cause'] = 'none'
    tb_events.loc[tb_events['from'].str.contains('positive_hiv'), 'cause'] = 'positive_hiv'    
    tb_events = tb_events.drop(columns=['from', 'to'])
    
    data = aggregate_risk_groups(tb_events)
    return sort_data(data).drop(columns=['cause'])
    
def aggregate_risk_groups(data):
    pop_filter = {
        'all_population': pd.Series(True, index=data.index),
        'u5_hhtb': (data.age == '0_to_5') & (data.hhtb == 'exposed'),
        'plwhiv': data.cause.str.contains('positive_hiv'),
    }
    drop = ['hhtb']
    groupby = ['draw', 'scenario', 'sex', 'year', 'age', 'cause', 'treatment_group']
    
    out = []
    for risk_group, group_filter in pop_filter.items():
        group_data = (data[group_filter]
                      .drop(columns=drop)
                      .groupby(groupby)
                      .sum()
                      .reset_index())
        group_data['risk_group'] = risk_group
        out.append(group_data)
    return pd.concat(out, ignore_index=True)


def aggregate_measure_by_cause(data):
    drop = ['cause']
    groupby = ['draw', 'scenario', 'sex', 'year', 'age', 'treatment_group', 'risk_group']
        
    tb = (data[data.cause.isin(['activetb_positive_hiv', 'activetb_susceptible_hiv'])]
          .drop(columns=drop)
          .groupby(groupby)
          .sum()
          .reset_index())
    tb['cause'] = 'all_form_tb'
    
    hiv_other = (data[data.cause.isin(['susceptible_tb_positive_hiv', 'ltbi_positive_hiv'])]
                 .drop(columns=drop)
                 .groupby(groupby)
                 .sum()
                 .reset_index())
    hiv_other['cause'] = 'hiv_other'
    
    other = data[data.cause == 'other_causes']
    
    return pd.concat([tb, hiv_other, other], ignore_index=True)


def split_and_aggregate_person_time(data):
    drop = ['cause']
    groupby = ['draw', 'scenario', 'sex', 'year', 'age', 'treatment_group', 'risk_group']
    
    ltbi_person_time = (data[data.cause.str.contains('ltbi')]
                        .drop(columns=drop)
                        .groupby(groupby)
                        .sum()
                        .reset_index())
    ltbi_person_time['cause'] = 'ltbi'
    
    all_person_time = (data
                       .drop(columns=drop)
                       .groupby(groupby)
                       .sum()
                       .reset_index())
    all_person_time['cause'] = 'all'
    
    return all_person_time, ltbi_person_time
    

def sort_data(data):
    column_order = ['scenario', 'risk_group', 'treatment_group', 'cause', 'age', 'sex', 'year', 'draw']
    return data.set_index(column_order).sort_index().reset_index()


def get_delta(data, join_columns):    
    baseline = (data[data.scenario == 'baseline']
                .drop(columns='scenario')
                .set_index(join_columns))
    
    delta = (baseline - data.set_index(join_columns + ['scenario'])).reset_index()
    
    return delta

def scale_data(base_data, data_to_scale, join_columns, agg_columns = [], py_multiplier=1):
    agg_data = (base_data
                .groupby(join_columns)
                .value.sum())
    scaled = (py_multiplier * data_to_scale.set_index(join_columns + agg_columns).value / agg_data).reset_index()
    # Remove numerical round off issues
    scaled.loc[np.abs(scaled.value) < 1e-10, 'value'] = 0
    return scaled

def pivot_and_summarize(data, index_columns, prefix=''):
    data = (data
            .set_index(index_columns + ['draw'])
            .unstack())
    data.columns = data.columns.droplevel()
    data.columns.name = None
    mean = data.mean(axis=1)
    # CI = 95%
    ub = np.percentile(data, 97.5, axis=1)
    lb = np.percentile(data, 2.5, axis=1)
    data[prefix + 'mean'] = mean
    data[prefix + 'ub'] = ub
    data[prefix + 'lb'] = lb
    data = data[[c for c in data.columns if isinstance(c, str)]]
    return data

def aggregate_over_treatment_group(data):
    groupby_cols = [c for c in data.columns if c not in ['treatment_group', 'value']]
    data = (data
            .groupby(groupby_cols)
            .value.sum()
            .reset_index())
    data['treatment_group'] = 'all'
    return data

def make_coverage_table(mdata: MeasureData):
    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = mdata.location
    pt['outcome'] = 'treatment_coverage'
    
    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'draw', 'treatment_group']
    delta = get_delta(pt, delta_join_columns)
    
    scale_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'draw', 'scenario']
    raw_scaled = scale_data(pt, pt, scale_join_columns, ['treatment_group'])
    delta_scaled = scale_data(pt, delta, scale_join_columns, ['treatment_group'])
    
    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_summary = pivot_and_summarize(raw_scaled, index_columns)
    delta_summary = pivot_and_summarize(delta_scaled, index_columns, prefix='averted_')
    
    return pd.concat([raw_summary, delta_summary], axis=1)

def make_tb_table(mdata: MeasureData):
    counts = mdata.tb_cases
    counts['location'] = mdata.location
    counts['outcome'] = 'actb_incidence_count'
    counts = aggregate_over_treatment_group(counts)  # FIXME: need to get the national count, not the simulation count
    
    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'draw']
    delta = get_delta(counts, delta_join_columns)
    
    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_summary = pivot_and_summarize(counts, index_columns)
    delta_summary = pivot_and_summarize(delta, index_columns, prefix='averted_')
    counts_summary = pd.concat([raw_summary, delta_summary], axis=1)
    
    counts['outcome'] = 'actb_incidence_rate'
    delta['outcome'] = 'actb_incidence_rate'
    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = mdata.location
    pt['outcome'] = 'actb_incidence_rate'
    pt = aggregate_over_treatment_group(pt)
    
    scale_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group','treatment_group', 'draw', 'scenario']
    raw_scaled = scale_data(pt, counts, scale_join_columns, py_multiplier=100_000)
    delta_scaled = scale_data(pt, delta, scale_join_columns, py_multiplier=100_000)
    
    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_scaled_summary = pivot_and_summarize(raw_scaled, index_columns)
    delta_scaled_summary = pivot_and_summarize(delta_scaled, index_columns, prefix='averted_')
    scaled_summary = pd.concat([raw_scaled_summary, delta_scaled_summary], axis=1)
    
    return pd.concat([counts_summary, scaled_summary])


def make_deaths_table(mdata: MeasureData):
    counts = mdata.deaths
    counts['location'] = mdata.location
    counts['outcome'] = 'deaths'
    counts = aggregate_over_treatment_group(counts)
    
    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'cause', 'draw']
    delta = get_delta(counts, delta_join_columns)
    
    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = mdata.location
    pt['outcome'] = 'deaths'
    pt = aggregate_over_treatment_group(pt)
    
    scale_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group','treatment_group', 'draw', 'scenario']
    raw_scaled = scale_data(pt, counts, scale_join_columns, ['cause'], py_multiplier=100_000)
    raw_scaled['outcome'] = raw_scaled.cause.apply(lambda x: 'deaths_due_to_' + x)
    raw_scaled = raw_scaled.drop(columns='cause')
    delta_scaled = scale_data(pt, delta, scale_join_columns, ['cause'], py_multiplier=100_000)
    delta_scaled['outcome'] = delta_scaled.cause.apply(lambda x: 'deaths_due_to_' + x)
    delta_scaled = delta_scaled.drop(columns='cause')
    
    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_scaled_summary = pivot_and_summarize(raw_scaled, index_columns)
    delta_scaled_summary = pivot_and_summarize(delta_scaled, index_columns, prefix='averted_')
    scaled_summary = pd.concat([raw_scaled_summary, delta_scaled_summary], axis=1)
    
    return scaled_summary

def make_dalys_table(mdata: MeasureData):
    ylls = mdata.ylls
    ylds = mdata.ylds
    cols = [c for c in ylls.columns if c != 'value']
    ylls = ylls.set_index(cols)
     # fill ylds due to other causes with 0
    counts = (ylls + ylds.set_index(cols).reindex(ylls.index).fillna(0)).reset_index()
    
    counts['location'] = mdata.location
    counts['outcome'] = 'dalys'
    counts = aggregate_over_treatment_group(counts)
    
    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'cause', 'draw']
    delta = get_delta(counts, delta_join_columns)
    
    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = mdata.location
    pt['outcome'] = 'dalys'
    pt = aggregate_over_treatment_group(pt)
    
    scale_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group','treatment_group', 'draw', 'scenario']
    raw_scaled = scale_data(pt, counts, scale_join_columns, ['cause'], py_multiplier=100_000)
    raw_scaled['outcome'] = raw_scaled.cause.apply(lambda x: 'dalys_due_to_' + x)
    raw_scaled = raw_scaled.drop(columns='cause')
    delta_scaled = scale_data(pt, delta, scale_join_columns, ['cause'], py_multiplier=100_000)
    delta_scaled['outcome'] = delta_scaled.cause.apply(lambda x: 'dalys_due_to_' + x)
    delta_scaled = delta_scaled.drop(columns='cause')
    
    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_scaled_summary = pivot_and_summarize(raw_scaled, index_columns)
    delta_scaled_summary = pivot_and_summarize(delta_scaled, index_columns, prefix='averted_')
    scaled_summary = pd.concat([raw_scaled_summary, delta_scaled_summary], axis=1)
    
    return scaled_summary

def make_person_time_table(mdata: MeasureData):
    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = mdata.location
    pt['outcome'] = 'person_time'
    pt = aggregate_over_treatment_group(pt)
    
    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'draw', 'treatment_group']
    delta = get_delta(pt, delta_join_columns)
    
    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_summary = pivot_and_summarize(pt, index_columns)
    delta_summary = pivot_and_summarize(delta, index_columns, prefix='averted_')
    
    return pd.concat([raw_summary, delta_summary], axis=1)

if __name__ == '__main__':
    output = []
    for location in ['ethiopia', 'india', 'peru', 'philippines', 'south_africa']:
        age_end = 'end_10'
        df10 = pd.read_hdf(output_dir + f'sim_raw_hdf/{location}_{age_end}_indexed.hdf').reset_index()

        age_end = 'end_100'
        df100 = pd.read_hdf(output_dir + f'sim_raw_hdf/{location}_{age_end}_indexed.hdf').reset_index()

        index_cols = ['draw', 'scenario', 'treatment_group', 'hhtb', 'age', 'sex', 'year', 'measure']
        df = make_raw_aggregates(df100.set_index(index_cols).add(df10.set_index(index_cols), fill_value=0).reset_index())
        measure_data = split_measures(df, location)
        loc_data = []
        for f in [make_coverage_table, make_tb_table, make_deaths_table, make_dalys_table, make_person_time_table]:
            loc_data.append(f(measure_data))
        output.append(pd.concat(loc_data))
    pd.concat(output).to_csv(output_dir + f'ltbi_final_results_merged_ages.csv')
    pd.concat(output).to_hdf(output_dir + f'ltbi_final_results_merged_ages.hdf', mode='w', key='data')

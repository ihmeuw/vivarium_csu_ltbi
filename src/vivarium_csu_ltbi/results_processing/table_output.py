import warnings
from typing import NamedTuple

import numpy as np
import pandas as pd

from vivarium_csu_ltbi.results_processing.counts_output import MeasureData
from vivarium_csu_ltbi.results_processing import utilities

warnings.filterwarnings('ignore')


class FinalData(NamedTuple):
    coverage: pd.DataFrame
    tb: pd.DataFrame
    deaths: pd.DataFrame
    dalys: pd.DataFrame
    person_time: pd.DataFrame
    averted: pd.DataFrame
    u5_hhtb_percent : pd.DataFrame
    aggregate: pd.DataFrame

    def dump(self, output_path):
        for name, df in self._asdict().items():
            df.to_hdf(str(output_path / f"{name}_final_table.hdf"), mode='w', key='data')
            df.to_csv(str(output_path / f"{name}_final_table.csv"))


def get_delta(data, join_columns):
    baseline = (data[data.scenario == 'baseline']
                .drop(columns='scenario')
                .set_index(join_columns))

    delta = (baseline - data.set_index(join_columns + ['scenario'])).reset_index()

    return delta


def scale_data(base_data, data_to_scale, join_columns, agg_columns=None, py_multiplier=1):
    if agg_columns is None:
        agg_columns = []

    agg_data = (base_data
                .groupby(join_columns)
                .value.sum())
    scaled = (py_multiplier * data_to_scale.set_index(join_columns + agg_columns).value / agg_data).reset_index()
    # Remove numerical round off issues
    scaled.loc[np.abs(scaled.value) < 1e-10, 'value'] = 0
    return scaled


def make_coverage_table(mdata: MeasureData, location: str):
    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = location
    pt['outcome'] = 'treatment_coverage'

    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'draw', 'treatment_group']
    delta = get_delta(pt, delta_join_columns)

    scale_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'draw', 'scenario']
    raw_scaled = scale_data(pt, pt, scale_join_columns, ['treatment_group'])
    delta_scaled = scale_data(pt, delta, scale_join_columns, ['treatment_group'])

    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_summary = utilities.pivot_and_summarize(raw_scaled, index_columns)
    delta_summary = utilities.pivot_and_summarize(delta_scaled, index_columns, prefix='averted_')

    return pd.concat([raw_summary, delta_summary], axis=1)


def make_tb_table(mdata: MeasureData, location: str):
    counts = mdata.tb_cases
    counts['location'] = location
    counts['outcome'] = 'actb_incidence_rate'
    counts = utilities.aggregate_over_treatment_group(counts)

    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'draw']
    delta = get_delta(counts, delta_join_columns)

    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = location
    pt['outcome'] = 'actb_incidence_rate'
    pt = utilities.aggregate_over_treatment_group(pt)
    
    scale_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group','treatment_group', 'draw', 'scenario']
    raw_scaled = scale_data(pt, counts, scale_join_columns, py_multiplier=100_000)
    delta_scaled = scale_data(pt, delta, scale_join_columns, py_multiplier=100_000)
    
    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_scaled_summary = utilities.pivot_and_summarize(raw_scaled, index_columns)
    delta_scaled_summary = utilities.pivot_and_summarize(delta_scaled, index_columns, prefix='averted_')
    scaled_summary = pd.concat([raw_scaled_summary, delta_scaled_summary], axis=1)

    population = mdata.national_population
    value_columns = ['mean', 'ub', 'lb', 'averted_mean', 'averted_ub', 'averted_lb']
    counts_summary = scaled_summary.reset_index()
    counts_summary['outcome'] = 'actb_incidence_count'
    counts_summary = pd.merge(counts_summary,
                              population,
                              how='inner',
                              on=['location', 'age', 'sex', 'risk_group'])
    counts_summary = counts_summary.loc[
        ~((counts_summary.year == 'all') | (counts_summary.age == 'all') | (counts_summary.sex == 'all'))
    ]
    counts_summary[value_columns] = (counts_summary[value_columns]
                                     .mul(counts_summary['population'], axis='index')
                                     .div(100_000))
    counts_summary.drop(columns='population', inplace=True)
    
    all_ages = (counts_summary
                .groupby([c for c in index_columns if c != 'age'])
                .sum()
                .reset_index())
    all_ages['age'] = 'all'
    counts_summary = pd.concat([counts_summary, all_ages])

    both_sexes = (counts_summary
                  .groupby([c for c in index_columns if c != 'sex'])
                  .sum()
                  .reset_index())
    both_sexes['sex'] = 'all'
    counts_summary = pd.concat([counts_summary, both_sexes])

    all_years = (counts_summary
                 .groupby([c for c in index_columns if c != 'year'])
                 .sum()
                 .reset_index())
    all_years['year'] = 'all'
    counts_summary = pd.concat([counts_summary, all_years])
    
    counts_summary = counts_summary.loc[
        ~((counts_summary.risk_group == 'u5_hhtb') & (counts_summary.age == 'all'))
    ]
    counts_summary = counts_summary.set_index(index_columns)[value_columns]
    
    return pd.concat([counts_summary, scaled_summary])


def make_deaths_table(mdata: MeasureData, location: str):
    counts = mdata.deaths
    counts['location'] = location
    counts['outcome'] = 'deaths'
    counts = utilities.aggregate_over_treatment_group(counts)

    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'cause', 'draw']
    delta = get_delta(counts, delta_join_columns)

    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = location
    pt['outcome'] = 'deaths'
    pt = utilities.aggregate_over_treatment_group(pt)

    scale_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'draw',
                          'scenario']
    raw_scaled = scale_data(pt, counts, scale_join_columns, ['cause'], py_multiplier=100_000)
    raw_scaled['outcome'] = raw_scaled.cause.apply(lambda x: 'deaths_due_to_' + x)
    raw_scaled = raw_scaled.drop(columns='cause')
    delta_scaled = scale_data(pt, delta, scale_join_columns, ['cause'], py_multiplier=100_000)
    delta_scaled['outcome'] = delta_scaled.cause.apply(lambda x: 'deaths_due_to_' + x)
    delta_scaled = delta_scaled.drop(columns='cause')

    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_scaled_summary = utilities.pivot_and_summarize(raw_scaled, index_columns)
    delta_scaled_summary = utilities.pivot_and_summarize(delta_scaled, index_columns, prefix='averted_')
    scaled_summary = pd.concat([raw_scaled_summary, delta_scaled_summary], axis=1)

    return scaled_summary


def make_dalys_table(mdata: MeasureData, location: str):
    ylls = mdata.ylls
    ylds = mdata.ylds
    cols = [c for c in ylls.columns if c != 'value']
    ylls = ylls.set_index(cols)
    # fill ylds due to other causes with 0
    counts = (ylls + ylds.set_index(cols).reindex(ylls.index).fillna(0)).reset_index()

    counts['location'] = location
    counts['outcome'] = 'dalys'
    counts = utilities.aggregate_over_treatment_group(counts)

    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'cause', 'draw']
    delta = get_delta(counts, delta_join_columns)

    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = location
    pt['outcome'] = 'dalys'
    pt = utilities.aggregate_over_treatment_group(pt)

    scale_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'draw',
                          'scenario']
    raw_scaled = scale_data(pt, counts, scale_join_columns, ['cause'], py_multiplier=100_000)
    raw_scaled['outcome'] = raw_scaled.cause.apply(lambda x: 'dalys_due_to_' + x)
    raw_scaled = raw_scaled.drop(columns='cause')
    delta_scaled = scale_data(pt, delta, scale_join_columns, ['cause'], py_multiplier=100_000)
    delta_scaled['outcome'] = delta_scaled.cause.apply(lambda x: 'dalys_due_to_' + x)
    delta_scaled = delta_scaled.drop(columns='cause')

    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_scaled_summary = utilities.pivot_and_summarize(raw_scaled, index_columns)
    delta_scaled_summary = utilities.pivot_and_summarize(delta_scaled, index_columns, prefix='averted_')
    scaled_summary = pd.concat([raw_scaled_summary, delta_scaled_summary], axis=1)

    return scaled_summary


def make_person_time_table(mdata: MeasureData, location: str):
    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = location
    pt['outcome'] = 'person_time'
    pt = utilities.aggregate_over_treatment_group(pt)

    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'draw', 'treatment_group']
    delta = get_delta(pt, delta_join_columns)

    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_summary = utilities.pivot_and_summarize(pt, index_columns)
    delta_summary = utilities.pivot_and_summarize(delta, index_columns, prefix='averted_')

    return pd.concat([raw_summary, delta_summary], axis=1)


def make_prop_under_five_hhtb_table(mdata: MeasureData, location: str):
    pt = mdata.person_time
    pt['location'] = location

    pt = pt.groupby(by=['scenario', 'location', 'risk_group',
                        'age', 'sex', 'year', 'draw']).sum().reset_index()

    pt = pt.loc[pt['age'] == '0_to_5']
    pt = pt.loc[pt['sex'] == 'all']

    pt = pt.set_index(['scenario', 'location', 'age', 'sex', 'year', 'draw'])

    under_five_population = pt.loc[pt['risk_group'] == 'all_population', 'value']

    under_five_prop_hhtb_population = pt.loc[pt['risk_group'] == 'u5_hhtb', 'value'] / under_five_population

    under_five_population = under_five_population.reset_index()
    under_five_population['outcome'] = 'under_five_population'

    under_five_prop_hhtb_population = under_five_prop_hhtb_population.reset_index()
    under_five_prop_hhtb_population['outcome'] = 'under_five_proportion_hhtb'

    index_columns = ['scenario', 'location', 'year', 'age', 'sex', 'outcome']
    return pd.concat([
        utilities.pivot_and_summarize(under_five_population, index_columns=index_columns),
        utilities.pivot_and_summarize(under_five_prop_hhtb_population, index_columns=index_columns)
    ], axis=0)


def make_tables(measure_data: MeasureData, location: str) -> FinalData:
    coverage = make_coverage_table(measure_data, location)
    tb = make_tb_table(measure_data, location)
    deaths = make_deaths_table(measure_data, location)
    dalys = make_dalys_table(measure_data, location)
    person_time = make_person_time_table(measure_data, location)
    u5_hhtb_percent = make_prop_under_five_hhtb_table(measure_data, location)
    aggregate = pd.concat([coverage, tb, deaths, dalys, person_time], axis=0)

    return FinalData(
        coverage=coverage,
        tb=tb,
        deaths=deaths,
        dalys=dalys,
        person_time=person_time,
        u5_hhtb_percent=u5_hhtb_percent,
        aggregate=aggregate
    )

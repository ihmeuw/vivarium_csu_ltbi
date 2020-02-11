import warnings
from typing import NamedTuple

import numpy as np
import pandas as pd

from vivarium_csu_ltbi.results_processing.counts_output import MeasureData

warnings.filterwarnings('ignore')


class FinalData(NamedTuple):
    coverage: pd.DataFrame
    tb: pd.DataFrame
    deaths: pd.DataFrame
    dalys: pd.DataFrame
    person_time: pd.DataFrame
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
    raw_summary = pivot_and_summarize(raw_scaled, index_columns)
    delta_summary = pivot_and_summarize(delta_scaled, index_columns, prefix='averted_')

    return pd.concat([raw_summary, delta_summary], axis=1)


def make_tb_table(mdata: MeasureData, location: str):
    counts = mdata.tb_cases
    counts['location'] = location
    counts['outcome'] = 'actb_incidence_rate'
    counts = aggregate_over_treatment_group(counts)

    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'draw']
    delta = get_delta(counts, delta_join_columns)

    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_summary = pivot_and_summarize(counts, index_columns)
    delta_summary = pivot_and_summarize(delta, index_columns, prefix='averted_')
    counts_summary = pd.concat([raw_summary, delta_summary], axis=1)

    counts['outcome'] = 'actb_incidence_rate'
    delta['outcome'] = 'actb_incidence_rate'
    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = location
    pt['outcome'] = 'actb_incidence_rate'
    pt = aggregate_over_treatment_group(pt)

    scale_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'draw',
                          'scenario']
    raw_scaled = scale_data(pt, counts, scale_join_columns, py_multiplier=100_000)
    delta_scaled = scale_data(pt, delta, scale_join_columns, py_multiplier=100_000)

    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_scaled_summary = pivot_and_summarize(raw_scaled, index_columns)
    delta_scaled_summary = pivot_and_summarize(delta_scaled, index_columns, prefix='averted_')
    scaled_summary = pd.concat([raw_scaled_summary, delta_scaled_summary], axis=1)

    population = mdata.national_population
    value_columns = ['mean', 'ub', 'lb', 'averted_mean', 'averted_ub', 'averted_lb']
    counts_summary = scaled_summary.reset_index()
    counts_summary['outcome'] = 'actb_incidence_count'
    counts_summary = pd.merge(counts_summary,
                              population,
                              how='inner',
                              on=['location', 'age', 'sex'])
    counts_summary[value_columns] = (counts_summary[value_columns]
                                     .mul(counts_summary['population'], axis='index')
                                     .div(100_000))
    counts_summary = counts_summary.drop(columns='population').set_index(index_columns)

    return pd.concat([counts_summary, scaled_summary])


def make_deaths_table(mdata: MeasureData, location: str):
    counts = mdata.deaths
    counts['location'] = location
    counts['outcome'] = 'deaths'
    counts = aggregate_over_treatment_group(counts)

    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'cause', 'draw']
    delta = get_delta(counts, delta_join_columns)

    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = location
    pt['outcome'] = 'deaths'
    pt = aggregate_over_treatment_group(pt)

    scale_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'draw',
                          'scenario']
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


def make_dalys_table(mdata: MeasureData, location: str):
    ylls = mdata.ylls
    ylds = mdata.ylds
    cols = [c for c in ylls.columns if c != 'value']
    ylls = ylls.set_index(cols)
    # fill ylds due to other causes with 0
    counts = (ylls + ylds.set_index(cols).reindex(ylls.index).fillna(0)).reset_index()

    counts['location'] = location
    counts['outcome'] = 'dalys'
    counts = aggregate_over_treatment_group(counts)

    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'cause', 'draw']
    delta = get_delta(counts, delta_join_columns)

    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = location
    pt['outcome'] = 'dalys'
    pt = aggregate_over_treatment_group(pt)

    scale_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'treatment_group', 'draw',
                          'scenario']
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


def make_person_time_table(mdata: MeasureData, location: str):
    pt = mdata.person_time.drop(columns='cause')
    pt['location'] = location
    pt['outcome'] = 'person_time'
    pt = aggregate_over_treatment_group(pt)

    delta_join_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'draw', 'treatment_group']
    delta = get_delta(pt, delta_join_columns)

    index_columns = ['outcome', 'location', 'year', 'age', 'sex', 'risk_group', 'scenario', 'treatment_group']
    raw_summary = pivot_and_summarize(pt, index_columns)
    delta_summary = pivot_and_summarize(delta, index_columns, prefix='averted_')

    return pd.concat([raw_summary, delta_summary], axis=1)


def make_tables(measure_data: MeasureData, location: str) -> FinalData:
    coverage = make_coverage_table(measure_data, location)
    tb = make_tb_table(measure_data, location)
    deaths = make_deaths_table(measure_data, location)
    dalys = make_dalys_table(measure_data, location)
    person_time = make_person_time_table(measure_data, location)
    aggregate = pd.concat([coverage, tb, deaths, dalys, person_time], axis=0)

    return FinalData(
        coverage=coverage,
        tb=tb,
        deaths=deaths,
        dalys=dalys,
        person_time=person_time,
        aggregate=aggregate
    )

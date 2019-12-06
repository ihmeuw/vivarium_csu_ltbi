from pathlib import Path
from typing import Union

from loguru import logger
import pandas as pd
import yaml

from vivarium_csu_ltbi import globals as ltbi_globals
from vivarium_csu_ltbi import paths as ltbi_paths

TEMPLATE_COLUMNS = ['cause', 'sex', 'age_group', 'measure', 'input_draw']


def get_results(location: str, timestamp: str = None) -> pd.DataFrame:
    """Retrieves the results for a country.

    Parameters
    ----------
    location
        The location to retrieve results for.
    timestamp
        Reference to the particular run. If not provided, this function
        will retrieve the latest results.

    Returns
    -------
    The results for the country if they exist.

    Raises
    ------
    FileNotFoundError
        If no results are found.

    Notes
    -----
    Results may not be complete! Verify that you have the correct number of
    rows in your outputs!

    """
    output_dir = ltbi_paths.get_output_directory(location, timestamp)
    return load_results_from_output_dir(output_dir)


def load_results_from_output_dir(output_dir: Union[str, Path], drop_missing=True) -> pd.DataFrame:
    """Loads results from an output directory.

    Parameters
    ----------
    output_dir
        The directory containing the simulation outputs. Expects a directory
        produced by ``psimulate``.
    drop_missing
        If true, drops draws with missing random seeds. Otherwise allow
        them to pass through.  In either case, warn if random seeds are
        missing.

    Returns
    -------
        The results in the output directory.

    Raises
    ------
    FileNotFoundError
        If the expected output files are not found.

    """
    output_dir = Path(output_dir).resolve()
    output_path = output_dir / 'output.hdf'
    keyspace_path = output_dir / 'keyspace.yaml'

    if not output_path.exists():
        raise FileNotFoundError(f'Cannot find output file at {str(output_path)}.')
    if not keyspace_path.exists():
        raise FileNotFoundError(f'Cannon find keyspace file at {str(keyspace_path)}')

    data = pd.read_hdf(output_path)  # type: pd.DataFrame
    with keyspace_path.open() as f:
        keyspace = yaml.load(f.read())

    # TODO: Count scenarios as well.
    seeds = len(keyspace['random_seed'])
    draws = len(keyspace['input_draw'])
    expected_result_count = seeds * draws
    result_count = len(data)

    if not expected_result_count == result_count:
        logger.warning(f'Results are incomplete. Expected {expected_result_count} rows of data in the results '
                       f'but only found {result_count}.')
        if drop_missing:
            logger.warning(f'Dropping draws with incomplete results.')
            expected_result_count_per_draw = seeds  # TODO: seeds * scenarios
            result_count_by_draw = data.groupby('input_draw').size()
            finished_draws = result_count_by_draw[result_count_by_draw == expected_result_count_per_draw].index.tolist()
            data = data.loc[data['input_draw'].isin(finished_draws)]
        else:
            logger.warning(f'Aggregating draws with incomplete results.')

    # TODO: Include scenarios in the groupby.
    data = data.groupby('input_draw').sum()

    return data


def get_sex_from_template(template_string: str):
    return template_string.split('_among_')[1].split('_in_')[0].capitalize()


def get_age_group_from_template(template_string: str):
    return template_string.split('_age_group_')[1]


def standardize_shape(data: pd.DataFrame, measure: str):
    """select specific measure of data and unpivot it into long-format dataframe"""
    measure_data = data[[c for c in data.columns if measure in c]]
    measure_data = measure_data.reset_index().melt(id_vars=['input_draw'], var_name=['label'])

    if 'due_to' in measure:
        measure, cause = measure.split('_due_to_', 1)
        measure_data['measure'] = measure
        measure_data['cause'] = cause
    else:
        measure_data['measure'] = measure
    measure_data['sex'] = measure_data.label.map(get_sex_from_template)
    measure_data['age_group'] = measure_data.label.map(get_age_group_from_template)

    measure_data.drop(columns='label', inplace=True)
    return measure_data


def get_disaggregated_results(data: pd.DataFrame, cause_names: list):
    """get disaggregated deaths, ylls, ylds, and dalys for each state"""
    deaths = []
    ylls = []
    ylds = []
    dalys = []
    for cause in cause_names:
        deaths.append(standardize_shape(data, f'death_due_to_{cause}'))
        ylls_sub = standardize_shape(data, f'ylls_due_to_{cause}')
        ylls.append(ylls_sub)
        if cause != 'other_causes':
            ylds_sub = standardize_shape(data, f'ylds_due_to_{cause}')
            dalys_sub = (ylds_sub.set_index([c for c in TEMPLATE_COLUMNS if c != 'measure']) +
                         ylls_sub.set_index([c for c in TEMPLATE_COLUMNS if c != 'measure'])).reset_index()
            dalys_sub['measure'] = 'dalys'
            ylds.append(ylds_sub)
            dalys.append(dalys_sub)
        else: # DALYs = YLLs if no YLDs for other_causes
            dalys_sub = (ylls_sub.set_index([c for c in TEMPLATE_COLUMNS if c != 'measure'])).reset_index()
            dalys_sub['measure'] = 'dalys'
            dalys.append(dalys_sub)

    death_data = pd.concat(deaths)
    yll_data = pd.concat(ylls)
    yld_data = pd.concat(ylds)
    daly_data = pd.concat(dalys)

    output = pd.concat([death_data, yll_data, yld_data, daly_data])
    output = output.set_index(TEMPLATE_COLUMNS).sort_index()

    return output.reset_index()


def append_demographic_aggregates(data: pd.DataFrame, by_cause=False):
    """aggregate results on demographic groups and append it to input data"""
    extra_cols = ['cause'] if by_cause else []

    age_aggregate = data.groupby(extra_cols + ['sex', 'measure', 'input_draw']).value.sum().reset_index()
    age_aggregate['age_group'] = 'all_ages'

    data = pd.concat([data, age_aggregate])

    sex_aggregate = data.groupby(extra_cols + ['age_group', 'measure', 'input_draw']).value.sum().reset_index()
    sex_aggregate['sex'] = 'Both'

    data = pd.concat([data, sex_aggregate])
    data = data.set_index(extra_cols + TEMPLATE_COLUMNS[1:]).sort_index()

    return data.reset_index()


def append_cause_aggregates(data: pd.DataFrame):
    """aggregate results on cause and append it to input data"""
    cause_aggregate = data.groupby(TEMPLATE_COLUMNS[1:]).value.sum().reset_index()
    cause_aggregate['cause'] = 'all_causes'

    data = pd.concat([data, cause_aggregate])

    return data.set_index(TEMPLATE_COLUMNS).sort_index().reset_index()


def get_person_time(data: pd.DataFrame):
    """aggregate person_time on demographic groups"""
    pt = standardize_shape(data, 'person_time')
    pt_agg = append_demographic_aggregates(pt, by_cause=False)
    pt_agg = pt_agg.drop(columns='measure').rename(columns={'value': 'person_time'})

    return pt_agg


def get_table_shell(results: pd.DataFrame, person_time: pd.DataFrame):
    """convert count space results to rate space and calculate mean, lower bound, and upper bound"""
    results_w_pt = pd.merge(results, person_time, on=['sex', 'age_group', 'input_draw'])
    results_w_pt.rename(columns={'value': 'count'}, inplace=True)
    results_w_pt['rate'] = results_w_pt['count'] / results_w_pt['person_time'] * 100_000

    g = results_w_pt.groupby(TEMPLATE_COLUMNS[:-1])[['count', 'rate', 'person_time']].describe(percentiles=[.025, .975])
    table_shell = g.filter([('count', 'mean'), ('count', '2.5%'), ('count', '97.5%'),
                            ('rate', 'mean'), ('rate', '2.5%'), ('rate', '97.5%'),
                            ('person_time', 'mean'), ('person_time', '2.5%'), ('person_time', '97.5%')])
    return table_shell


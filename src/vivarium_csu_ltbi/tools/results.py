from pathlib import Path
from typing import Tuple

import yaml
import pandas as pd
from loguru import logger

import vivarium_csu_ltbi.paths as ltbi_paths
from vivarium_csu_ltbi import globals as project_globals
from vivarium_csu_ltbi.data import counts_output, table_output


def process_latest_results(model_versions: Tuple[str] = None, location: str = None,
                           preceding_results_num: int = 0, output_path: str = None):
    if len(model_versions) != 1 or len(model_versions) != 2:
        raise ValueError("Please pass either one or two model versions")
    if (model_versions and location is None) or (location and model_versions is None):
        raise ValueError("Please pass both model version(s) and a location")

    location = location.lower()
    results_paths = {mv: find_most_recent_results(mv, location, preceding_results_num) for mv in model_versions}

    results_name = "_".join(model_versions) + f'_{location}'
    timestamps = "_".join([results_paths[mv].stem for mv in model_versions])
    if not output_path:
        output_path = (Path(".") / results_name / timestamps).resolve()
    else:
        output_path = (Path(output_path) / results_name / timestamps).resolve()
    output_path.mkdir(exist_ok=True)

    logger.info("Loading and generating count-space data by measure.")
    if len(model_versions) == 2:
        model_1 = load_data(results_paths[model_versions[0]])
        model_2 = load_data(results_paths[model_versions[1]])
        # TODO: Generalize to all non-draw columns?
        idx_cols = ['draw', 'scenario', 'treatment_group', 'hhtb', 'age', 'sex', 'year', 'measure']
        model_sum = model_1.set_index(idx_cols).add(model_2.set_index(idx_cols), fill_value=0.).reset_index()
        count_aggregates = counts_output.get_raw_counts(model_sum)
    else:
        data = load_data(results_paths[model_versions[0]])
        count_aggregates = counts_output.get_raw_counts(data)

    measure_data = counts_output.split_measures(count_aggregates)

    logger.info("Writing count-space data by measure.")
    for measure in measure_data._fields:  # Is there a non-intrusive way to do this?
        getattr(measure_data, measure).to_hdf(str(output_path / f"{measure}_count_data.hdf"),
                                              mode='w', key='data')
        getattr(measure_data, measure).to_csv(str(output_path / f"{measure}_count_data.csv"))

    logger.info("Calculating data for the final results table.")
    final_tables = table_output.make_tables(measure_data, location)

    # TODO: write out each table separately and aggregated

    logger.info("Writing data for the final results table to csv and hdf formats.")
    final_tables.to_hdf(str(output_path / f"final_results.hdf"), mode='w', key='data')
    final_tables.to_csv(str(output_path / f"final_results.csv"))


def process_specific_results(model_output_paths: Tuple[str], output_path: str):
    model_output_paths = [Path(mop) for mop in model_output_paths]
    for mop in model_output_paths:
        if not confirm_output_directory(mop):
            raise ValueError("Please pass output paths to output directories containing an "
                             "output.hdf file.")
    raise NotImplementedError


def find_most_recent_results(model_version: str, location: str, preceding_results_num: int = 0) -> Path:
    logger.info(f"Searching for most recent results relevant to {model_version} and {location}.")
    output_runs = Path(ltbi_paths.RESULT_DIRECTORY) / model_version / location

    if not output_runs.exists() or len(list(output_runs.iterdir())) == 0:
        raise FileNotFoundError(f"No results present in {output_runs}.")

    try:
        most_recent_run_dir = sorted(output_runs.iterdir())[-1 - preceding_results_num]  # yields full path
    except IndexError:
        logger.error(f"{1 + preceding_results_num} sets of results don't exist.")
        raise IndexError

    if not (most_recent_run_dir / 'output.hdf').exists():
        raise FileNotFoundError(f"No data yet written for most recent run {most_recent_run_dir}f")

    logger.info(f"Most recent results found at {most_recent_run_dir}.")
    return most_recent_run_dir


def get_random_seeds(results_directory: Path) -> list:
    with open(results_directory / 'keyspace.yaml') as f:
        data = yaml.load(f)
    return data['random_seed']


def find_common_subset(df: pd.DataFrame, expected_seeds: list, has_scenarios: bool) -> pd.DataFrame:
    num_obs = df.shape[0]

    valid_groups = []
    df = df.reset_index(['random_seed'])
    for draw, g in df.groupby(['input_draw']):
        if set(df['random_seed']) == set(expected_seeds):
            if has_scenarios:  # We have an additional criterion
                if len(df['scenario'].unique()) == project_globals.NUM_SCENARIOS:
                    valid_groups.append(g)
            else:  # no additional criteria, seeds are all we need to check
                valid_groups.append(g)

    df = pd.concat(valid_groups, axis=0)
    df = df.set_index('random_seed', append=True)

    logger.info(f"{num_obs} total simulations in this output. {len(df)} represent full sets of "
                f"scenarios and random seeds.")

    return df


def confirm_output_directory(out_dir: Path) -> bool:
    return 'output.hdf' in list(out_dir.iterdir())


def load_data(results_path: Path) -> pd.DataFrame:
    has_scenarios = False
    if hasattr(project_globals, 'SCENARIO_COLUMN'):
        has_scenarios = True

    expected_seeds = get_random_seeds(results_path)
    df = pd.read_hdf(results_path / 'output.hdf')

    df = df.drop(columns=['random_seed', 'input_draw'])  # FIXME these are unintended dupes
    df.index = df.index.set_names('input_draw', level=0)

    if has_scenarios:
        df = df.rename(columns={project_globals.SCENARIO_COLUMN: 'scenario'})

    df = find_common_subset(df, expected_seeds, has_scenarios)

    df = df.reset_index()
    df = df.drop(columns=['random_seed'])
    idx_columns = ['input_draw', 'scenario'] if has_scenarios else ['input_draw']
    df = df.groupby(idx_columns).sum()

    return df

import functools
from pathlib import Path
from typing import Tuple, Dict

import pandas as pd
from loguru import logger

import vivarium_csu_ltbi.paths as ltbi_paths
from vivarium_csu_ltbi import globals as project_globals
from vivarium_csu_ltbi.results_processing import counts_output, table_output


def validate_process_latest_results_args(model_versions: Tuple[str], location: str):
    if not (len(model_versions) == 1 or len(model_versions) == 2):
        raise ValueError("Please pass either one or two model versions")
    if (model_versions and location is None) or (location and model_versions is None):
        raise ValueError("Please pass both model version(s) and a location")


def process_latest_results(model_versions: Tuple[str] = None, location: str = None,
                           preceding_results_num: int = 0, output_path: str = None):
    validate_process_latest_results_args(model_versions, location)

    location = project_globals.formatted_location(location)
    results_paths = {mv: find_most_recent_results(mv, location, preceding_results_num) for mv in model_versions}
    output_path = get_output_path(model_versions, location, results_paths, output_path)

    # =========================================================================>
    # A series of transformations to the data mapped to arbitrary numbers of
    # results.
    logger.info("Loading model results.")
    raw_model_data = {mv: load_data(results_paths[mv]) for mv in model_versions}

    logger.info("Filtering to common subset of seeds.")
    complete_seeds_by_result = {mv: get_common_seeds(df) for mv, df in raw_model_data.items()}
    seed_intersection = set.intersection(*complete_seeds_by_result.values())
    subset_model_data = {mv: df.loc[df['random_seed'].isin(seed_intersection)] for mv, df in raw_model_data.items()}

    logger.info("Summing across seeds.")
    summed_model_data = {mv: sum_over_seeds(df) for mv, df in subset_model_data.items()}

    logger.info("Formatting the data.")
    formatted_model_data = {mv: counts_output.format_data(df) for mv, df in summed_model_data.items()}

    logger.info("Combining the model results.")
    summed_model_data = functools.reduce(counts_output.sum_model_results, formatted_model_data.values())

    logger.info("Generating and dumping count-space data.")
    count_space_data = counts_output.get_raw_counts(summed_model_data)
    measure_data = counts_output.split_measures(count_space_data, location)
    measure_data.dump(output_path)

    logger.info("Generating and dumping final output table data.")
    final_data = table_output.make_tables(measure_data, location)
    final_data.dump(output_path)


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


def get_common_seeds(df: pd.DataFrame) -> set:
    seed_sets = []
    for draw, g in df.groupby(['input_draw']):
        if 'scenario' in g.columns:  # We have an additional criterion
            if len(df['scenario'].unique()) == project_globals.NUM_SCENARIOS:
                seed_sets.append(set(g['random_seed'].unique()))
        else:  # no additional criteria, seeds are all we need to check
            seed_sets.append(set(g['random_seed'].unique()))
    common_seeds = set.intersection(*seed_sets)

    return common_seeds


def sum_over_seeds(df: pd.DataFrame):
    df = df.reset_index()
    df = df.drop(columns=['random_seed'])
    df = df.groupby(['input_draw', 'scenario']).sum()
    return df


def load_data(results_path: Path) -> pd.DataFrame:
    df = pd.read_hdf(results_path / 'output.hdf')
    df = df.reset_index(drop=True)  # the index is duplicated in columns
    df = df.rename(columns={project_globals.SCENARIO_COLUMN: 'scenario'})
    df = df.set_index(['input_draw', 'random_seed', 'scenario'])
    return df.reset_index()


def get_output_path(model_versions: Tuple[str], location: str,
                    results_paths: Dict[str, Path], output_path: str) -> Path:
    results_name = "_".join(model_versions) + f'_{location}_model_results'
    timestamps = "_and_".join([results_paths[mv].stem for mv in model_versions])

    if not output_path:
        output_path = (Path(".") / results_name / timestamps).resolve()
    else:
        output_path = (Path(output_path) / results_name / timestamps).resolve()
    output_path.mkdir(exist_ok=True, parents=True)

    return output_path

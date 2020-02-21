import functools
import yaml
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


def process_latest_results(model_versions: Tuple[str], location: str,
                           preceding_results_num: int = 0, output_path: str = None):
    """Implements the make_results click entrypoint. model_versions and location are required arguments."""
    validate_process_latest_results_args(model_versions, location)

    location = project_globals.formatted_location(location)
    results_paths = {mv: find_most_recent_results(mv, location, preceding_results_num) for mv in model_versions}
    output_path = get_output_path(model_versions, location, results_paths, output_path)

    # =========================================================================>
    # A series of transformations to the data mapped to arbitrary numbers of
    # results.
    logger.info("Loading model results.")
    raw_model_data = {mv: load_data(results_paths[mv]) for mv in model_versions}
    merged_keyspace = get_keyspace_union(results_paths)

    logger.info("Filtering to common subset of seeds.")
    complete_data_by_result = {mv: get_complete_draws(data, merged_keyspace) for mv, data in raw_model_data.items()}

    common_seeds, common_draws = merge_complete_data(complete_data_by_result, merged_keyspace)
    if (len(common_seeds) == 0) or (len(common_draws) == 0):
        logger.error("No overlapping results to process.")
        raise RuntimeError("No overlapping results to process.")
    subset_model_data = {mv: df.loc[(df['random_seed'].isin(common_seeds))
                                    & (df['input_draw'].isin(common_draws))] for mv, df in raw_model_data.items()}

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


def get_complete_draws(df: pd.DataFrame, merged_keyspace: dict) -> dict:
    """For each draw-seed combination, we keep only those seeds that have data for all scenarios."""
    complete_draws = {}
    # for draw in merged_keyspace[project_globals.INPUT_DRAW_COLUMN]:
    # results are coming in slowly for draws 10-20, prohibiting results generation.
    # we will subset to the first 10 draws, which are complete.
    for draw in [946, 650, 232, 357, 394, 602, 629, 29, 680, 829]:
        draw_data = df.loc[df[project_globals.INPUT_DRAW_COLUMN] == draw]
        scenario_count = draw_data.groupby(by=['random_seed'])['scenario'].count() == project_globals.NUM_SCENARIOS
        scenario_count = scenario_count.loc[scenario_count]
        if not scenario_count.empty:
            complete_draws[draw] = set(scenario_count.reset_index()['random_seed'].unique())

    return complete_draws


def merge_complete_data(data: dict, merged_keyspace: dict) -> Tuple:
    # get common draws
    common_draws = set(merged_keyspace[project_globals.INPUT_DRAW_COLUMN])
    for model in data.keys():
        model_draws = set(data[model].keys())
        common_draws = common_draws.intersection(model_draws)

    # get intersection of seeds in common draws
    common_seeds = set(merged_keyspace[project_globals.RANDOM_SEED_COLUMN])
    for model in data.keys():
        for draw in common_draws:
            common_seeds = common_seeds.intersection(data[model][draw])

    return common_seeds, common_draws


def sum_over_seeds(df: pd.DataFrame):
    df = df.reset_index()
    df = df.drop(columns=['random_seed'])
    df = df.groupby(['input_draw', 'scenario']).sum()

    return df


def load_data(results_path: Path) -> pd.DataFrame:
    df = pd.read_hdf(results_path / 'output.hdf')
    df = df.reset_index(drop=True)  # the index is duplicated in columns
    df = df.rename(columns={project_globals.SCENARIO_COLUMN: 'scenario'})

    return df


def load_keyspace(results_path: Path) -> pd.DataFrame:
    with (results_path / 'keyspace.yaml').open() as f:
        keyspace = yaml.full_load(f)

    return keyspace


def get_keyspace_union(results_paths: dict) -> dict:
    model_keyspaces = {m: load_keyspace(rp) for m, rp in results_paths.items()}
    models = list(results_paths.keys())
    keys = model_keyspaces[models[0]].keys()
    merged = {}
    for k in keys:
        key_sets = [set(model_keyspaces[m][k]) for m in models]
        merged[k] = set.union(*key_sets)

    return merged


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

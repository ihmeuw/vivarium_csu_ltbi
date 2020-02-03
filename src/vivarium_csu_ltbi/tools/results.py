from pathlib import Path

import yaml
import pandas as pd
from loguru import logger

import vivarium_csu_ltbi.paths as ltbi_paths
from vivarium_csu_ltbi.data import counts_output, table_output


def main(scenario: str = None, location: str = None, preceding_results_num: int = 0,
         model_outputs_path: str = None, output_path: str = None, ):
    """This is to be used as a click entrypoint, see cli.py. It must have one of
    (scenario, location) or model_outputs_path."""
    location = location.lower()
    validate_make_results_arguments(scenario, location, model_outputs_path)

    if model_outputs_path:
        results_path = Path(model_outputs_path)
    else:
        results_path = find_most_recent_results(scenario, location, preceding_results_num)

    if not output_path:
        output_path = Path(f"./{scenario}_{location}_results").resolve()
    else:
        output_path = Path(output_path) / f"./{scenario}_{location}_results"
    output_path.mkdir(exist_ok=True)

    df = load_data(results_path)
    logger.info("Generating count-space data by measure.")
    measure_data = counts_output.split_measures(df)

    logger.info("Writing count-space data by measure.")
    for measure in measure_data._fields:  # Is there a non-intrusive way to do this?
        getattr(measure_data, measure).to_hdf(str(output_path / f"{scenario}_{location}_{measure}_count_data.hdf"), key='data')
        getattr(measure_data, measure).to_csv(str(output_path / f"{scenario}_{location}_{measure}_count_data.csv"))

    logger.info("Calculating data for the final results table.")
    final_tables = table_output.make_tables(measure_data, location)

    logger.info("Writing data for the final results table to csf and hdf formats.")
    final_tables.to_hdf(str(output_path / f"{scenario}_{location}_final_results.hdf"), key='data')
    final_tables.to_csv(str(output_path / f"{scenario}_{location}_final_results.csv"))


def validate_make_results_arguments(scenario, location, model_outputs_path):
    if scenario is None and location is None and model_outputs_path is None:
        raise ValueError("Please pass either a scenario and a location or a model outputs path. You passed none.")
    if model_outputs_path:
        if scenario or location:
            raise ValueError("Please pass either a scenario and a location or a model outputs path, not both.")
    if scenario and location is None:
        raise ValueError("When passing a scenario, please pass a location as well.")
    elif location and scenario is None:
        raise ValueError("When passing a location, please pass a scenario as well.")


def find_most_recent_results(scenario: str, location: str, preceding_results_num: int = 0) -> Path:
    logger.info(f"Searching for most recent results relevant to {scenario} and {location}.")
    output_runs = Path(ltbi_paths.RESULT_DIRECTORY) / scenario / location

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


def find_common_subset(df: pd.DataFrame, expected_seeds: list) -> pd.DataFrame:
    num_obs = df.shape[0]

    valid_groups = []
    df = df.reset_index(['random_seed'])
    for draw, g in df.groupby(['input_draw']):
        if ((set(df['random_seed']) == set(expected_seeds))
           & (len(df['scenario'].unique()) == 3)):
            valid_groups.append(g)

    df = pd.concat(valid_groups, axis=0)
    df = df.set_index('random_seed', append=True)

    logger.info(f"{num_obs} total simulations in this output. {len(df)} represent full sets of "
                f"scenarios and random seeds.")

    return df


def load_data(results_path: Path) -> pd.DataFrame:
    expected_seeds = get_random_seeds(results_path)
    df = pd.read_hdf(results_path / 'output.hdf')

    df = df.drop(columns=['random_seed', 'input_draw'])
    df.index = df.index.set_names('input_draw', level=0)
    df = df.rename(columns={'ltbi_treatment_scale_up.scenario': 'scenario'})

    df = find_common_subset(df, expected_seeds)

    df = df.reset_index()
    df = df.drop(columns=['random_seed'])
    df = df.groupby(['input_draw', 'scenario']).sum()

    return df

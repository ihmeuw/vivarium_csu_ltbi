"""LTBI project input and output paths."""
from pathlib import Path
from typing import Union

import vivarium_csu_ltbi
from vivarium_csu_ltbi import globals as ltbi_globals

BASE_DIR = Path(vivarium_csu_ltbi.__file__).resolve().parent
ARTIFACT_ROOT = BASE_DIR / 'artifacts'
HOUSEHOLD_TB_ARTIFACT_ROOT = ARTIFACT_ROOT / "household_tb"
LTBI_INCIDENCE_ARTIFACT_ROOT = ARTIFACT_ROOT / "ltbi_incidence"

RESULT_DIRECTORY = Path(f'/share/costeffectiveness/results/{ltbi_globals.PROJECT_NAME}/')


def get_hh_tb_input_data_path(location):
    formatted_location = ltbi_globals.formatted_location(location)
    file_names = {
        'south_africa': 'South_Africa',
        'india': 'India',
        'philippines': 'Philippines',
        'ethiopia': 'Ethiopia',
        'peru': 'Peru'
    }
    data_path = Path("/home/j/Project/simulation_science/latent_tuberculosis_infection/"
                     "literature/household_structure/microdata/")
    input_data_file = data_path / f"{file_names[formatted_location]}.dta"
    return input_data_file


def get_hh_tb_input_artifact_path(location):
    formatted_location = ltbi_globals.formatted_location(location)
    input_path = HOUSEHOLD_TB_ARTIFACT_ROOT / 'input'
    input_path.mkdir(parents=True, exist_ok=True)
    input_file = input_path / f'{formatted_location}.hdf'
    return input_file


def get_hh_tb_output_artifact_path(location):
    formatted_location = ltbi_globals.formatted_location(location)
    return HOUSEHOLD_TB_ARTIFACT_ROOT / f'{formatted_location}.hdf'


def get_hh_tb_intermediate_output_dir_path(location):
    formatted_location = ltbi_globals.formatted_location(location)
    output_path = HOUSEHOLD_TB_ARTIFACT_ROOT / 'output' / f'{formatted_location}'
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path


def get_ltbi_inc_input_artifact_path(location):
    formatted_location = ltbi_globals.formatted_location(location)
    input_path = LTBI_INCIDENCE_ARTIFACT_ROOT / 'input'
    input_path.mkdir(parents=True, exist_ok=True)
    input_file = input_path / f'{formatted_location}.hdf'
    return input_file


def get_ltbi_inc_output_artifact_path(location):
    formatted_location = ltbi_globals.formatted_location(location)
    return LTBI_INCIDENCE_ARTIFACT_ROOT / f'{formatted_location}.hdf'


def get_ltbi_inc_intermediate_output_dir_path(location):
    formatted_location = ltbi_globals.formatted_location(location)
    output_path = LTBI_INCIDENCE_ARTIFACT_ROOT / 'output' / f'{formatted_location}'
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path


def get_final_artifact_path(location):
    formatted_location = ltbi_globals.formatted_location(location)
    return ARTIFACT_ROOT / f'{formatted_location}.hdf'


def get_output_directory(location: str, timestamp: Union[str, None]):
    """Converts a location name and a timestamp into a results directory path.

    Parameters
    ----------
    location
        The location name. Should be a proper name as specified in the
        GBD reporting location hierarchy.
    timestamp
        Reference to the particular run. If not provided, this function
        produce the path to the latest results.

    Returns
    -------
    A path to the requested vivarium results.

    Raises
    ------
    FileNotFoundError
        If the results directory indicated by the arguments does not exist.

    """
    if not RESULT_DIRECTORY.exists():
        raise FileNotFoundError(f'Cannot find the results directory {str(RESULT_DIRECTORY)}. '
                                f'Are you on the cluster?  Make sure you have access to the share drive.')
    sanitized_location = ltbi_globals.formatted_location(location)
    location_directory = RESULT_DIRECTORY / 'subminimal-with-risk' / sanitized_location
    location_results = sorted(location_directory.iterdir(), key=lambda path: path.name)
    if timestamp is not None:
        if timestamp not in location_results:
            raise FileNotFoundError(f'No results found at {str(location_directory / timestamp)}. Make sure the '
                                    f'timestamp you specified exists in the location results directory.')
        output_dir = location_directory / timestamp
    else:
        output_dir = location_directory / location_results[-1]
    return output_dir

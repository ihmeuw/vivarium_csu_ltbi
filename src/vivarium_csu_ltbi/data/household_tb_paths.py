from pathlib import Path

from vivarium_csu_ltbi.data.globals import formatted_country


ARTIFACT_PATH = Path("/share/costeffectiveness/artifacts/vivarium_csu_ltbi/household_tb")


def get_input_data_path(country):
    file_names = {
        'south_africa': 'South_Africa',
        'india': 'India',
        'philippines': 'Philippines',
        'ethiopia': 'Ethiopia',
        'brazil': 'Brazil'
    }
    data_path = Path("/home/j/Project/simulation_science/latent_tuberculosis_infection/literature/household_structure/"
                     "microdata/")
    input_data_file = data_path / f"{file_names[formatted_country(country)]}.dta"
    return input_data_file


def get_input_artifact_path(country):
    input_path = ARTIFACT_PATH / 'input'
    input_path.mkdir(parents=True, exist_ok=True)
    input_file = input_path / f'{formatted_country(country)}.hdf'
    return input_file


def get_output_artifact_path(country):
    return ARTIFACT_PATH / f'{formatted_country(country)}.hdf'


def get_intermediate_output_dir_path(country):
    output_path = ARTIFACT_PATH / 'output' / f'{formatted_country(country)}'
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path

from pathlib import Path

ARTIFACT_PATH = Path("/share/costeffectiveness/artifacts/vivarium_csu_ltbi/ltbi_incidence")


def formatted_country(country):
    return country.replace(" ", "_").lower()


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

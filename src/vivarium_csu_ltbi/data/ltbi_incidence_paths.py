from pathlib import Path

ARTIFACT_PATH = Path("/share/costeffectiveness/artifacts/vivarium_csu_ltbi/ltbi_incidence")


def get_input_artifact_path(country):
    input_path = ARTIFACT_PATH / 'input'
    input_path.mkdir(parents=True, exist_ok=True)
    input_file = input_path / f'{country.replace(" ", "_").lower()}.hdf'
    return input_file

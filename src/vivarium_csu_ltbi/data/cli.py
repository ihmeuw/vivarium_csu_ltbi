import click
from loguru import logger

from vivarium_public_health.dataset_manager.artifact import Artifact
from vivarium_cluster_tools.psimulate.utilities import get_drmaa

from vivarium_csu_ltbi.data.ltbi_incidence_model import load_data
from vivarium_csu_ltbi.data.ltbi_incidence_paths import get_input_artifact_path

drmaa = get_drmaa()

COUNTRIES = ['South Africa', 'India', 'Philippines', 'Ethiopia', 'Brazil']


@click.command()
def get_ltbi_incidence_input_data():
    """Collect the data necessary to model incidence using dismod and save
    it to an Artifact. This severs our database dependency and avoids
    num_countries * 1k simultaneous requests."""
    for country in COUNTRIES:
        logger.info(f"Processing {country}.")
        input_artifact_path = get_input_artifact_path(country)
        if input_artifact_path.is_file():
            input_artifact_path.unlink()

        art = Artifact(str(input_artifact_path))

        logger.info("Pulling data.")
        p_ltbi, f_ltbi, csmr_all = load_data(country)

        logger.info(f"Writing data to {art}.")
        art.write('cause.latent_tuberculosis_infection.prevalence', p_ltbi)
        art.write('cause.latent_tuberculosis_infection.excess_mortality', f_ltbi)
        art.write('cause.all_causes.cause_specific_mortality_rate', csmr_all)

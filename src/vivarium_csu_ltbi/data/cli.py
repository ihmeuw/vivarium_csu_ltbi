import shutil
import os

import click
from loguru import logger

from vivarium_public_health.dataset_manager.artifact import Artifact
from vivarium_cluster_tools.psimulate.utilities import get_drmaa

from vivarium_csu_ltbi.data.ltbi_incidence_model import load_data
from vivarium_csu_ltbi.data.ltbi_incidence_paths import get_input_artifact_path
import vivarium_csu_ltbi.data.ltbi_incidence_scripts as script

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
        art.write('cause.ltbi_hiv.cause_specific_mortality_rate', csmr_all)


@click.command()
@click.argument("country", type=click.Choice(COUNTRIES))
def get_ltbi_incidence_parallel(country):
    """Launch jobs to calculate 1k draws of ltbi incidence for a country and
    collect it in a single artifact.
    """
    with drmaa.Session() as s:
        jt = s.createJobTemplate()
        jt.remoteCommand = shutil.which('python')
        jt.args = [script.__file__, "estimate_ltbi_incidence", country]
        jt.nativeSpecification = ("-V -b y -P proj_cost_effect -q all.q -l fmem=1G -l fthread=1 -l h_rt=3:00:00 "
                                  "-N get_ltbi_incidence")
        jids = s.runBulkJobs(jt, 1, 1000, 1)
        parent_jid = jids[0].split('.')[0]
        logger.info(f"Submitted array job ({parent_jid}) for calculating LTBI incidence.")
        jt.delete()

        jt = s.createJobTemplate()
        jt.workingDirectory = os.getcwd()
        jt.remoteCommand = shutil.which('python')
        jt.args = [script.__file__, "collect_ltbi_incidence", country]
        jt.nativeSpecification = ("-V -b y -P proj_cost_effect -q all.q -l fmem=4G -l fthread=1 -l h_rt=3:00:00 "
                                  f"-N collect_ltbi_incidence -hold_jid {parent_jid}")
        jid = s.runJob(jt)
        logger.info(f"Submitted hold job ({jid}) for aggregating LTBI incidence.")
        jt.delete()

import shutil
import os

import click
from loguru import logger

from vivarium_public_health.dataset_manager.artifact import Artifact
from vivarium_cluster_tools.psimulate.utilities import get_drmaa

from vivarium_csu_ltbi.data.ltbi_incidence_model import load_data
from vivarium_csu_ltbi.data.ltbi_incidence_paths import (get_input_artifact_path, get_intermediate_output_dir_path,
                                                         get_output_artifact_path)
import vivarium_csu_ltbi.data.ltbi_incidence_scripts as script

drmaa = get_drmaa()

COUNTRIES = ['South Africa', 'India', 'Philippines', 'Ethiopia', 'Brazil']


@click.command()
def get_ltbi_incidence_input_data():
    """Collect the data necessary to model incidence using dismod and save
    it to an Artifact. This severs our database dependency and avoids
    num_countries * 1k simultaneous requests."""
    for country in COUNTRIES:
        logger.info(f"Removing old results for {country}.")  # to avoid stale data
        input_artifact_path = get_input_artifact_path(country)
        if input_artifact_path.is_file():
            input_artifact_path.unlink()

    for country in COUNTRIES:
        logger.info(f"Processing {country}.")
        input_artifact_path = get_input_artifact_path(country)
        art = Artifact(str(input_artifact_path))

        logger.info("Pulling data.")
        p_ltbi, f_ltbi, csmr_all = load_data(country)

        logger.info(f"Writing data to {input_artifact_path}.")
        art.write('cause.latent_tuberculosis_infection.prevalence', p_ltbi)
        art.write('cause.latent_tuberculosis_infection.excess_mortality', f_ltbi)
        art.write('cause.all_causes.cause_specific_mortality_rate', csmr_all)


@click.command()
@click.argument("country", type=click.Choice(COUNTRIES))
def get_ltbi_incidence_parallel(country):
    """Launch jobs to calculate 1k draws of LTBI incidence for `country`` and
    collect it in a single artifact.
    """
    logger.info(f"Removing old results for {country}.")  # to avoid stale data
    intermediate_output_path = get_intermediate_output_dir_path(country)
    for f in intermediate_output_path.iterdir():
        f.unlink()
    output_artifact_path = get_output_artifact_path(country)
    if output_artifact_path.is_file():
        output_artifact_path.unlink()

    with drmaa.Session() as s:
        jt = s.createJobTemplate()
        jt.remoteCommand = shutil.which('python')
        jt.args = [script.__file__, "estimate_ltbi_incidence", country]
        jt.nativeSpecification = ("-V -b y -P proj_cost_effect -q all.q -l fmem=1G -l fthread=1 -l h_rt=5:00:00 "
                                  f"-N {country}_gltbi_inc")
        jids = s.runBulkJobs(jt, 1, 1000, 1)
        parent_jid = jids[0].split('.')[0]
        logger.info(f"Submitted array job ({parent_jid}) for calculating LTBI incidence in {country}.")
        jt.delete()

        jt = s.createJobTemplate()
        jt.workingDirectory = os.getcwd()
        jt.remoteCommand = shutil.which('python')
        jt.args = [script.__file__, "collect_ltbi_incidence", country]
        jt.nativeSpecification = ("-V -b y -P proj_cost_effect -q all.q -l fmem=4G -l fthread=1 -l h_rt=5:00:00 "
                                  f"-N {country}_cltbi_inc -hold_jid {parent_jid}")
        jid = s.runJob(jt)
        logger.info(f"Submitted hold job ({jid}) for aggregating LTBI incidence in {country}.")
        jt.delete()


@click.command()
@click.argument("country", type=click.Choice(COUNTRIES))
def restart_ltbi_incidence_parallel(country):
    """Examine existing LTBI incidence data for `country` and submit jobs for
    any missing draws that may be present."""

    intermediate_output_path = get_intermediate_output_dir_path(country)
    logger.info(f"Looking for missing draws in {intermediate_output_path}.")

    exists = [int(f.stem.split('.')[0]) for f in intermediate_output_path.iterdir()]
    should = list(range(1000))
    missing = set(should).difference(set(exists))

    if not missing:
        logger.info("No missing draws found. Existing now.")
        return

    logger.info(f"Missing draws identified: {missing}.")
    with drmaa.Session() as s:
        jids = []

        jt = s.createJobTemplate()
        jt.remoteCommand = shutil.which('python')
        jt.args = [script.__file__, "estimate_ltbi_incidence", country]
        for draw in missing:
            jt.nativeSpecification = (f"-v SGE_TASK_ID={int(draw)+1} -b y -P proj_cost_effect -q all.q -l fmem=1G "
                                      f"-l fthread=1 -l h_rt=5:00:00 -N {country}_gltbi_inc")
            jid = s.runJob(jt)
            jids.append(jid)
            logger.info(f"Submitted job ({jid}) for calculating LTBI incidence for draw {draw} in {country}.")
        jt.delete()

        jt = s.createJobTemplate()
        jt.workingDirectory = os.getcwd()
        jt.remoteCommand = shutil.which('python')
        jt.args = [script.__file__, "collect_ltbi_incidence", country]
        jt.nativeSpecification = ("-V -b y -P proj_cost_effect -q all.q -l fmem=4G -l fthread=1 -l h_rt=5:00:00 "
                                  f"-N {country}_cltbi_inc -hold_jid {','.join(jids)}")
        jid = s.runJob(jt)
        logger.info(f"Submitted hold job ({jid}) for aggregating LTBI incidence in {country}.")
        jt.delete()

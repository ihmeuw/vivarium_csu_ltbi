import shutil
import os

import click
from loguru import logger

from vivarium import Artifact
from vivarium_cluster_tools.psimulate.utilities import get_drmaa
from vivarium_csu_ltbi.data.globals import COUNTRIES, formatted_country
from vivarium_csu_ltbi.data import ltbi_incidence_paths, ltbi_incidence_model
import vivarium_csu_ltbi.data.ltbi_incidence_scripts as ltbi_script
from vivarium_csu_ltbi.data import household_tb_paths, household_tb_model
import vivarium_csu_ltbi.data.household_tb_scripts as hh_tb_script

drmaa = get_drmaa()


@click.command()
def get_ltbi_incidence_input_data():
    """Collect the data necessary to model incidence using dismod and save
    it to an Artifact. This severs our database dependency and avoids
    num_countries * 1k simultaneous requests."""
    for country in COUNTRIES:
        logger.info(f"Removing old LTBI incidence input data for {country}.")  # to avoid stale data
        input_artifact_path = ltbi_incidence_paths.get_input_artifact_path(country)
        if input_artifact_path.is_file():
            input_artifact_path.unlink()

    for country in COUNTRIES:
        logger.info(f"Processing {country}.")
        input_artifact_path = ltbi_incidence_paths.get_input_artifact_path(country)
        art = Artifact(str(input_artifact_path))

        logger.info("Pulling data.")
        p_ltbi, f_ltbi, csmr_all = ltbi_incidence_model.load_data(country)

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
    logger.info(f"Removing old LTBI incidence results for {country}.")  # to avoid stale data
    intermediate_output_path = ltbi_incidence_paths.get_intermediate_output_dir_path(country)
    for f in intermediate_output_path.iterdir():
        f.unlink()
    output_artifact_path = ltbi_incidence_paths.get_output_artifact_path(country)
    if output_artifact_path.is_file():
        output_artifact_path.unlink()

    with drmaa.Session() as s:
        jt = s.createJobTemplate()
        jt.remoteCommand = shutil.which('python')
        jt.args = [ltbi_script.__file__, "estimate_ltbi_incidence", country]
        jt.nativeSpecification = ("-V -b y -P proj_cost_effect -q all.q -l fmem=1G -l fthread=1 -l h_rt=5:00:00 "
                                  f"-N {formatted_country(country)}_gltbi_inc")
        jids = s.runBulkJobs(jt, 1, 1000, 1)
        parent_jid = jids[0].split('.')[0]
        logger.info(f"Submitted array job ({parent_jid}) for calculating LTBI incidence in {country}.")
        jt.delete()

        jt = s.createJobTemplate()
        jt.workingDirectory = os.getcwd()
        jt.remoteCommand = shutil.which('python')
        jt.args = [ltbi_script.__file__, "collect_ltbi_incidence", country]
        jt.nativeSpecification = ("-V -b y -P proj_cost_effect -q all.q -l fmem=4G -l fthread=1 -l h_rt=5:00:00 "
                                  f"-N {formatted_country(country)}_cltbi_inc -hold_jid {parent_jid}")
        jid = s.runJob(jt)
        logger.info(f"Submitted hold job ({jid}) for aggregating LTBI incidence in {country}.")
        jt.delete()


@click.command()
@click.argument("country", type=click.Choice(COUNTRIES))
def restart_ltbi_incidence_parallel(country):
    """Examine existing LTBI incidence data for `country` and submit jobs for
    any missing draws that may be present."""

    intermediate_output_path = ltbi_incidence_paths.get_intermediate_output_dir_path(country)
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
        jt.args = [ltbi_script.__file__, "estimate_ltbi_incidence", country]
        for draw in missing:
            jt.nativeSpecification = (f"-v TASK_ID={int(draw)+1} -b y -P proj_cost_effect -q all.q -l fmem=1G "
                                      f"-l fthread=1 -l h_rt=5:00:00 -N {formatted_country(country)}_gltbi_inc")
            jid = s.runJob(jt)
            jids.append(jid)
            logger.info(f"Submitted job ({jid}) for calculating LTBI incidence for draw {draw} in {country}.")
        jt.delete()

        jt = s.createJobTemplate()
        jt.workingDirectory = os.getcwd()
        jt.remoteCommand = shutil.which('python')
        jt.args = [ltbi_script.__file__, "collect_ltbi_incidence", country]
        jt.nativeSpecification = ("-V -b y -P proj_cost_effect -q all.q -l fmem=4G -l fthread=1 -l h_rt=5:00:00 "
                                  f"-N {formatted_country(country)}_cltbi_inc -hold_jid {','.join(jids)}")
        jid = s.runJob(jt)
        logger.info(f"Submitted hold job ({jid}) for aggregating LTBI incidence in {country}.")
        jt.delete()


@click.command()
def get_household_tb_input_data():
    for country in COUNTRIES:
        logger.info(f"Removing old household TB input data for {country}.")  # to avoid stale data
        input_artifact_path = household_tb_paths.get_input_artifact_path(country)
        if input_artifact_path.is_file():
            input_artifact_path.unlink()

    for country in COUNTRIES:
        logger.info(f"Processing {country}.")
        input_artifact_path = household_tb_paths.get_input_artifact_path(country)
        art = Artifact(str(input_artifact_path))

        logger.info("Pulling data.")
        household_data = household_tb_model.load_household_input_data(country)
        actb_prevalence = household_tb_model.load_actb_prevalence_input_data(country)

        logger.info(f"Writing data to {input_artifact_path}.")
        art.write('household_data.estimate', household_data)
        art.write('cause.activate_tuberculosis.prevalence', actb_prevalence)


@click.command()
@click.argument("country", type=click.Choice(COUNTRIES))
def get_household_tb_parallel(country):
    logger.info(f"Removing old household TB results for {country}.")  # to avoid stale data
    intermediate_output_path = household_tb_paths.get_intermediate_output_dir_path(country)
    for f in intermediate_output_path.iterdir():
        f.unlink()
    output_artifact_path = household_tb_paths.get_output_artifact_path(country)
    if output_artifact_path.is_file():
        output_artifact_path.unlink()

    with drmaa.Session() as s:
        jt = s.createJobTemplate()
        jt.remoteCommand = shutil.which('python')
        jt.args = [hh_tb_script.__file__, "estimate_household_tb", country]
        jt.nativeSpecification = ("-V -b y -P proj_cost_effect -q all.q -l fmem=4G -l fthread=1 -l h_rt=2:00:00 "
                                  f"-N {formatted_country(country)}_ghh_tb_exp")
        jids = s.runBulkJobs(jt, 1, 1000, 1)
        parent_jid = jids[0].split('.')[0]
        logger.info(f"Submitted array job ({parent_jid}) for estimating household TB exposure in {country}.")
        jt.delete()

        jt = s.createJobTemplate()
        jt.workingDirectory = os.getcwd()
        jt.remoteCommand = shutil.which('python')
        jt.args = [hh_tb_script.__file__, "collect_household_tb", country]
        jt.nativeSpecification = ("-V -b y -P proj_cost_effect -q all.q -l fmem=8G -l fthread=1 -l h_rt=2:00:00 "
                                  f"-N {formatted_country(country)}_chh_tb_exp -hold_jid {parent_jid}")
        jid = s.runJob(jt)
        logger.info(f"Submitted hold job ({jid}) for aggregating household TB exposure in {country}.")
        jt.delete()

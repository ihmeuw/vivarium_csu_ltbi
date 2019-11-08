import os

import pandas as pd
from loguru import logger

from vivarium_public_health.dataset_manager.artifact import Artifact

from vivarium_csu_ltbi.data.ltbi_incidence_model import format_for_dismod, fit_and_predict, format_for_art
from vivarium_csu_ltbi.data.ltbi_incidence_paths import (get_input_artifact_path, get_output_artifact_path,
                                                         get_intermediate_output_dir_path)


KNOTS = list(range(0, 101, 5))


def estimate_ltbi_incidence(country, draw):
    """"""
    input_artifact_path = get_input_artifact_path(country)
    logger.info(f"Loading input data from {input_artifact_path}.")
    art = Artifact(str(input_artifact_path))
    p_ltbi = art.load('cause.latent_tuberculosis_infection.prevalence')
    f_ltbi = art.load('cause.latent_tuberculosis_infection.excess_mortality')
    csmr_all = art.load('cause.all_causes.cause_specific_mortality_rate')

    output = []
    for sex in ['Female', 'Male']:
        for year in range(1990, 2018):
            logger.info(f"Modeling LTBI incidence for draw: {draw}, sex: {sex} and year: {year}")
            p = format_for_dismod(p_ltbi, draw, sex, year, 'p')
            f = format_for_dismod(f_ltbi, draw, sex, year, 'f')
            m_all = format_for_dismod(csmr_all, draw, sex, year, 'm_all')
            i_ltbi = fit_and_predict(p, f, m_all, KNOTS)
            output.append(format_for_art(i_ltbi, draw, country, sex, year))
    intermediate_output_path = get_intermediate_output_dir_path(country)
    logger.info(f"Writing results to {intermediate_output_path}.")
    df = pd.concat(output, axis=0)
    df.to_hdf(intermediate_output_path / f'{draw}.hdf', 'data')


def collect_ltbi_incidence(country):
    """Aggregate the results of LTBI incidence modeling into a single HDF file."""
    intermediate_output_path = get_intermediate_output_dir_path(country)
    logger.info(f"Reading results from {intermediate_output_path}")
    data = []
    for f in intermediate_output_path.iterdir():
        data.append(pd.read_hdf(f))
    data = pd.concat(data, axis=0).reset_index()
    output_artifact_path = get_output_artifact_path(country)
    logger.info(f"Writing results to {output_artifact_path}.")
    art = Artifact(str(output_artifact_path))
    art.write("cause.latent_tuberculosis_infection.incidence", data)


if __name__ == "__main__":
    import sys
    func = sys.argv[1]
    country = sys.argv[2]
    if func == 'estimate_ltbi_incidence':
        draw = int(os.environ['SGE_TASK_ID']) - 1
        estimate_ltbi_incidence(country, draw)
    elif func == 'collect_ltbi_incidence':
        collect_ltbi_incidence(country)
    else:
        raise ValueError(f"Bad first argument: {func}. Must be 'estimate_ltbi_incidence' or 'collect_ltbi_incidence'.")

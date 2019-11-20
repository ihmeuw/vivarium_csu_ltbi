import os

import pandas as pd
import numpy as np
from loguru import logger

from vivarium import Artifact
from vivarium_csu_ltbi.data import household_tb_model, household_tb_paths


def estimate_household_tb(country: str, draw: int, year_start=2017):
    """for certain country in year 2017,
    sweep over draw
    """

    input_artifact_path = household_tb_paths.get_input_artifact_path(country)
    intermediate_output_path = household_tb_paths.get_intermediate_output_dir_path(country)

    logger.info(f"Loading input data from {input_artifact_path}.")
    art = Artifact(input_artifact_path)
    df_hh = art.load("household_data.estimate")
    hh_ids = df_hh.hh_id.unique()
    prev_actb = art.load("cause.activate_tuberculosis.prevalence")

    logger.info(f"Estimating household TB for draw: {draw}.")
    sample_hhids = np.random.choice(hh_ids, size=len(hh_ids), replace=True)
    df_hh_sample = pd.DataFrame()
    for i in sample_hhids:
        df_hh_sample = df_hh_sample.append(df_hh[df_hh.hh_id == i])

    data = household_tb_model.interpolation(prev_actb, df_hh_sample, year_start, draw)
    res = household_tb_model.age_sex_specific_actb_prop(data)
    res['location'] = country
    res['year_start'] = year_start
    res['year_end'] = year_start + 1
    res['draw'] = draw

    index_cols = ['location', 'sex', 'age_group_start', 'year_start',
                  'age_group_end', 'year_end', 'draw']
    res = res.set_index(index_cols)

    logger.info(f"Writing results to {intermediate_output_path}.")
    res.to_hdf(intermediate_output_path / f'{draw}.hdf', 'data')


def collect_household_tb(country: str):
    intermediate_output_path = household_tb_paths.get_intermediate_output_dir_path(country)

    logger.info(f"Reading results from {intermediate_output_path}")
    data = []
    for f in intermediate_output_path.iterdir():
        data.append(pd.read_hdf(f))
    data = pd.concat(data, axis=0).reset_index()
    output_artifact_path = household_tb_paths.get_output_artifact_path(country)

    logger.info(f"Writing results to {output_artifact_path}.")
    art = Artifact(str(output_artifact_path))
    art.write("risk_factor.household_tuberculosis.exposure", data)


if __name__ == '__main__':
    import sys
    func = sys.argv[1]
    country = sys.argv[2]
    if func == 'estimate_household_tb':
        if 'SGE_TASK_ID' in os.environ and os.environ['SGE_TASK_ID'] != 'undefined':
            draw = int(os.environ['SGE_TASK_ID']) - 1
        elif 'TASK_ID' in os.environ:
            draw = int(os.environ['TASK_ID']) - 1
        else:
            raise ValueError("No task number given")
        estimate_household_tb(country, draw)
    elif func == 'collect_household_tb':
        collect_household_tb(country)
    else:
        raise ValueError(f"Bad first argument: {func}. Must be 'estimate_ltbi_incidence' or 'collect_ltbi_incidence'.")

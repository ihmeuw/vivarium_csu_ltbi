from pathlib import Path

import pandas as pd
from loguru import logger

import vivarium_csu_ltbi.paths as ltbi_paths


def main(scenario, location):
    df = load_data(scenario, location)
    formatted_df = format_data(df)
    import pdb
    pdb.set_trace()


def find_most_recent_results(scenario, location):
    output_runs = Path(ltbi_paths.RESULT_DIRECTORY) / scenario / location

    if not output_runs.exists() or len(list(output_runs.iterdir())) == 0:
        raise ValueError(f"No results present in {output_runs}.")

    most_recent_results_dir = None
    for run_dir in sorted(output_runs.iterdir(), reverse=True):
        if (output_runs / run_dir / 'output.hdf').is_file():
            most_recent_results_dir = run_dir
            # TODO: Parse output.hdf and ensure results ?
            #       it's possible to have an output.hdf but no intersecting data
            break

    if most_recent_results_dir is None:
        raise ValueError(f"No results present in {output_runs}.")

    return most_recent_results_dir


def parse_output_hdf(results_directory: Path):
    # TODO: Determine how many seeds / draws should be present
    #       select only those draws that have all seeds

    pass


def load_data(country_path):
    # TODO: PARSE RESULTS DIR
    #       find most recent results, report on finish status
    #       pare down to common set, report that too
    df = pd.read_hdf(country_path + '/output.hdf')
    df = df.drop(columns='random_seed').reset_index()
    df.rename(columns={'ltbi_treatment_scale_up.scenario': 'scenario'},
              inplace=True)

    scenario_count = (df
                      .groupby(['input_draw', 'random_seed'])
                      .scenario
                      .count())
    idx_completed = scenario_count[scenario_count == 3].reset_index()
    idx_completed = idx_completed[['input_draw', 'random_seed']]
    df_completed = pd.merge(df,
                            idx_completed,
                            how='inner',
                            on=['input_draw', 'random_seed'])
    df_completed = df_completed.groupby(['input_draw', 'scenario']).sum()

    return df_completed


def get_year_from_template(template_string):
    return template_string.split('_among_')[0].split('_')[-1]


def get_sex_from_template(template_string):
    return template_string.split('_among_')[1].split('_in_')[0]


def get_age_group_from_template(template_string):
    return '_'.join(template_string.split('_age_group_')[1].split('_treatment_group_')[0].split('_')[:-3])


def get_risk_group_from_template(template_string):
    return template_string.split('_to_hhtb_')[0].split('_')[-1]


def get_treatment_group_from_template(template_string):
    return template_string.split('_treatment_group_')[1]


def get_measure_from_template(template_string):
    return template_string.split('_in_')[0]


def format_data(df):
    idx_cols = ['draw', 'scenario', 'treatment_group', 'hhtb', 'age', 'sex', 'year', 'measure']
    items = ['death', 'ylls', 'ylds', 'event_count', 'prevalent_cases', 'person_time', 'population_point_estimate']
    wanted_cols = []
    for i in df.columns:
        for j in items:
            if j in i:
                wanted_cols.append(i)

    df = df[wanted_cols]
    df = (df
          .reset_index()
          .melt(id_vars=['input_draw', 'scenario'],
                var_name='label'))
    df['year'] = df.label.map(get_year_from_template)
    df['sex'] = df.label.map(get_sex_from_template)
    df['age'] = df.label.map(get_age_group_from_template)
    df['hhtb'] = df.label.map(get_risk_group_from_template)
    df['treatment_group'] = df.label.map(get_treatment_group_from_template)
    df['measure'] = df.label.map(get_measure_from_template)
    df = (df
          .rename(columns={'input_draw': 'draw'})
          .drop(columns='label')
          .set_index(idx_cols))

    return df


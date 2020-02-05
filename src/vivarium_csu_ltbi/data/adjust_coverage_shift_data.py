from pathlib import Path
import pandas as pd


def read_data(source_csv: str):
    path = Path(__file__).resolve().parent / source_csv
    return pd.read_csv(str(path))


def write_data(df: pd.DataFrame, dest_csv: str):
    path = Path(__file__).resolve().parent / dest_csv
    df.to_csv(path, index=False)


def eliminate_3hp_under_two(df: pd.DataFrame):
    """3HP is not approved for use in kids < 2. We adjust the scale-up to
    account for this."""
    df = df.set_index(['location', 'treatment_subgroup', 'year'])

    under_2 = df['age_end'] == 2.0
    scenario_3hp_scale_up = df['scenario'] == '3HP_scale_up'
    medication_6h = df['medication'] == '6H'
    medication_3hp = df['medication'] == '3HP'

    df.loc[under_2 & scenario_3hp_scale_up & medication_6h, 'value'] += df.loc[under_2 & scenario_3hp_scale_up & medication_3hp, 'value']
    df.loc[under_2 & scenario_3hp_scale_up & medication_3hp, 'value'] = 0
    return df.reset_index()


def expand_to_age(df: pd.DataFrame):
    under_2 = df
    under_2['age_start'] = 0.0
    under_2['age_end'] = 2.0

    over_2 = df.copy()
    over_2['age_start'] = 2.0
    over_2['age_end'] = 125.0

    return pd.concat([under_2, over_2]).reset_index(drop=True)


if __name__ == "__main__":
    coverage_shift = read_data("intervention_coverage_shift_raw.csv")
    coverage_shift_expanded = expand_to_age(coverage_shift)
    coverage_shift_adjusted = eliminate_3hp_under_two(coverage_shift_expanded)
    write_data(coverage_shift_adjusted, "intervention_coverage_shift.csv")

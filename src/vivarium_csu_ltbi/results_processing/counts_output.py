from typing import Tuple, NamedTuple

import pandas as pd

from vivarium_csu_ltbi.results_processing import utilities


class MeasureData(NamedTuple):
    deaths: pd.DataFrame
    person_time: pd.DataFrame
    ltbi_person_time: pd.DataFrame
    ylls: pd.DataFrame
    ylds: pd.DataFrame
    tb_cases: pd.DataFrame
    national_population: pd.DataFrame

    def dump(self, output_path):
        for name, df in self._asdict().items():
            df.to_hdf(str(output_path / f"{name}_count_data.hdf"), mode='w', key='data')
            df.to_csv(str(output_path / f"{name}_count_data.csv"))


def get_year_from_template(template_string: str) -> str:
    return template_string.split('_among_')[0].split('_')[-1]


def get_sex_from_template(template_string: str) -> str:
    return template_string.split('_among_')[1].split('_in_')[0]


def get_age_group_from_template(template_string: str) -> str:
    return '_'.join(template_string.split('_age_group_')[1].split('_treatment_group_')[0].split('_')[:-3])


def get_risk_group_from_template(template_string: str) -> str:
    return template_string.split('_to_hhtb_')[0].split('_')[-1]


def get_treatment_group_from_template(template_string: str) -> str:
    return template_string.split('_treatment_group_')[1]


def get_measure_from_template(template_string: str) -> str:
    return template_string.split('_in_')[0]


def format_data(df: pd.DataFrame) -> pd.DataFrame:
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
    df = df.rename(columns={'input_draw': 'draw'}).drop(columns='label')

    return df


def sum_model_results(df: pd.DataFrame, other: pd.DataFrame) -> pd.DataFrame:
    index_cols = ['draw', 'scenario', 'treatment_group', 'hhtb', 'age', 'sex', 'year', 'measure']
    summation = df.set_index(index_cols).add(other.set_index(index_cols), fill_value=0).reset_index()
    return summation


def get_raw_counts(data: pd.DataFrame) -> pd.DataFrame:
    labels = {'0_to_5': ['early_neonatal', 'late_neonatal', 'post_neonatal', '1_to_4'],
              '5_to_15': ['5_to_9', '10_to_14'],
              '15_to_60': ['15_to_19', '20_to_24', '25_to_29',
                           '30_to_34', '35_to_39', '40_to_44',
                           '45_to_49', '50_to_54', '55_to_59', ],
              '60+': ['60_to_64', '65_to_69', '70_to_74', '75_to_79',
                      '80_to_84', '85_to_89', '90_to_94', '95_plus']}
    age_aggregates = []
    for group, ages in labels.items():
        age_group = data[data.age.isin(ages)]
        age_group = (age_group
                     .drop(columns=['age'])
                     .groupby(['draw', 'scenario', 'treatment_group', 'hhtb', 'sex', 'year', 'measure'])
                     .sum()
                     .reset_index())
        age_group['age'] = group
        age_aggregates.append(age_group)
    data = pd.concat(age_aggregates)

    all_ages = (data
                .groupby(['draw', 'scenario', 'treatment_group', 'hhtb', 'sex', 'year', 'measure'])
                .value.sum()
                .reset_index())
    all_ages['age'] = 'all'
    data = pd.concat([data, all_ages])

    both_sexes = (data
                  .groupby(['draw', 'scenario', 'treatment_group', 'hhtb', 'age', 'year', 'measure'])
                  .value.sum()
                  .reset_index())
    both_sexes['sex'] = 'all'
    data = pd.concat([data, both_sexes])

    all_years = (data[data.year.isin(['2020', '2021', '2022', '2023', '2024'])]
                 .groupby(['draw', 'scenario', 'treatment_group', 'hhtb', 'sex', 'age', 'measure'])
                 .value.sum()
                 .reset_index())
    all_years['year'] = 'all'
    data = pd.concat([data, all_years])

    return data


def get_measure(data: pd.DataFrame, measure: str) -> pd.DataFrame:
    assert measure in ['death', 'ylls', 'ylds']
    data = data[data.measure.str.contains(measure)]
    data['cause'] = data.measure.str.split('due_to_').str[1]
    data = data.drop(columns=['measure'])

    data = aggregate_risk_groups(data)
    data = aggregate_measure_by_cause(data)
    return sort_data(data)


def get_person_time(data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    person_time = data[data.measure.str.contains('_person_time')]
    person_time['cause'] = person_time.measure.str.split('_person_time').str[0]
    person_time = person_time.drop(columns='measure')

    data = aggregate_risk_groups(person_time)
    all_person_time, ltbi_person_time = split_and_aggregate_person_time(data)
    return sort_data(all_person_time), sort_data(ltbi_person_time)


def get_tb_events(data: pd.DataFrame) -> pd.DataFrame:
    event_counts = data[data.measure.str.contains('event_count')]
    event_counts['from'] = event_counts.measure.str.split('_to_').str[0]
    event_counts['to'] = event_counts.measure.str.split('_to_').str[1].str.split('_event_count').str[0]
    event_counts = event_counts.drop(columns='measure')

    tb_events = (event_counts[(event_counts['from'].str.contains('ltbi'))
                              & (event_counts['to'].str.contains('activetb'))])
    tb_events['cause'] = 'none'
    tb_events.loc[tb_events['from'].str.contains('positive_hiv'), 'cause'] = 'positive_hiv'
    tb_events = tb_events.drop(columns=['from', 'to'])

    data = aggregate_risk_groups(tb_events)
    return sort_data(data).drop(columns=['cause'])


def get_national_population(location: str) -> pd.DataFrame:
    from db_queries import get_ids, get_population

    age_group_ids = list(range(2, 21)) + [30, 31, 32, 235]
    age_table = get_ids('age_group')
    age_table = age_table[age_table.age_group_id.isin(age_group_ids)]
    age_table['age_group_name'] = age_table.age_group_name.map(lambda x: x.replace(' ', '_').lower())
    age_group_dict = dict(zip(age_table.age_group_id, age_table.age_group_name))

    location_dict = {'ethiopia': 179, 'india': 163, 'peru': 123, 'philippines': 16, 'south_africa': 196}
    pop = get_population(location_id=location_dict[location],
                         age_group_id=age_group_ids,
                         sex_id=[1, 2],
                         gbd_round_id=5)
    pop['age'] = pop.age_group_id.map(age_group_dict)
    pop['sex'] = pop.sex_id.map({1: 'male', 2: 'female'})
    data = pop[['age', 'sex', 'population']]

    labels = {'0_to_5': ['early_neonatal', 'late_neonatal', 'post_neonatal', '1_to_4'],
              '5_to_15': ['5_to_9', '10_to_14'],
              '15_to_60': ['15_to_19', '20_to_24', '25_to_29',
                           '30_to_34', '35_to_39', '40_to_44',
                           '45_to_49', '50_to_54', '55_to_59', ],
              '60+': ['60_to_64', '65_to_69', '70_to_74', '75_to_79',
                      '80_to_84', '85_to_89', '90_to_94', '95_plus']}
    age_aggregates = []
    for group, ages in labels.items():
        age_group = data[data.age.isin(ages)]
        age_group = (age_group
                     .groupby(['sex'])
                     .sum()
                     .reset_index())
        age_group['age'] = group
        age_aggregates.append(age_group)
    data = pd.concat(age_aggregates)

    all_ages = (data
                .groupby(['sex'])
                .sum()
                .reset_index())
    all_ages['age'] = 'all'
    data = pd.concat([data, all_ages])

    both_sexes = (data
                  .groupby(['age'])
                  .sum()
                  .reset_index())
    both_sexes['sex'] = 'all'
    data = pd.concat([data, both_sexes])

    data['location'] = location
    return data


def get_risk_specific_population(data: pd.DataFrame, location: str) -> pd.DataFrame:
    pt, ltbi_pt = get_person_time(data)
    pt['location'] = location
    pt = utilities.aggregate_over_treatment_group(pt)
    index_cols = ['location', 'year', 'age', 'sex', 'cause', 'risk_group', 'scenario', 'treatment_group']
    pt = utilities.pivot_and_summarize(pt, index_cols)
    # 2019 baseline mean person_time
    pt = (pt
          .reset_index()
          .query('year == "2019" & scenario == "baseline"')
          .filter(['location', 'age', 'sex', 'risk_group', 'mean']))
    
    pt_cols = ['location', 'age', 'sex']
    pt_plwhiv = (pt
                 .loc[pt.risk_group == 'plwhiv']
                 .drop(columns='risk_group')
                 .set_index(pt_cols))
    pt_u5_hhtb = (pt
                  .loc[pt.risk_group == 'u5_hhtb']
                  .drop(columns='risk_group')
                  .set_index(pt_cols))
    pt_all_population = (pt
                         .loc[pt.risk_group == 'all_population']
                         .drop(columns='risk_group')
                         .set_index(pt_cols))
    
    plwhiv_prop = pt_plwhiv / pt_all_population
    u5_hhtb_prop = pt_u5_hhtb / pt_all_population.query('age == "0_to_5"')
    
    population = get_national_population(location)
    population = population.rename(columns={'population': 'mean'}).set_index(pt_cols)
    
    plwhiv_pop = (plwhiv_prop * population).reset_index()
    plwhiv_pop['risk_group'] = 'plwhiv'
    
    u5_hhtb_pop = (u5_hhtb_prop * population.query('age == "0_to_5"')).reset_index()
    u5_hhtb_pop['risk_group'] = 'u5_hhtb'
    
    all_pop = population.reset_index()
    all_pop['risk_group'] = 'all_population'
    
    df = pd.concat([all_pop, plwhiv_pop, u5_hhtb_pop], ignore_index=True)
    df.rename(columns={'mean': 'population'}, inplace=True)
    return df


def aggregate_risk_groups(data: pd.DataFrame) -> pd.DataFrame:
    pop_filter = {
        'all_population': pd.Series(True, index=data.index),
        'u5_hhtb': (data.age == '0_to_5') & (data.hhtb == 'exposed'),
        'plwhiv': data.cause.str.contains('positive_hiv'),
    }
    drop = ['hhtb']
    groupby = ['draw', 'scenario', 'sex', 'year', 'age', 'cause', 'treatment_group']

    out = []
    for risk_group, group_filter in pop_filter.items():
        group_data = (data[group_filter]
                      .drop(columns=drop)
                      .groupby(groupby)
                      .sum()
                      .reset_index())
        group_data['risk_group'] = risk_group
        out.append(group_data)
    return pd.concat(out, ignore_index=True)


def aggregate_measure_by_cause(data: pd.DataFrame) -> pd.DataFrame:
    drop = ['cause']
    groupby = ['draw', 'scenario', 'sex', 'year', 'age', 'treatment_group', 'risk_group']

    tb = (data[data.cause.isin(['activetb_positive_hiv', 'activetb_susceptible_hiv'])]
          .drop(columns=drop)
          .groupby(groupby)
          .sum()
          .reset_index())
    tb['cause'] = 'all_form_tb'

    hiv_other = (data[data.cause.isin(['susceptible_tb_positive_hiv', 'ltbi_positive_hiv'])]
                 .drop(columns=drop)
                 .groupby(groupby)
                 .sum()
                 .reset_index())
    hiv_other['cause'] = 'hiv_other'

    other = data[data.cause == 'other_causes']

    return pd.concat([tb, hiv_other, other], ignore_index=True)


def split_and_aggregate_person_time(data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    drop = ['cause']
    groupby = ['draw', 'scenario', 'sex', 'year', 'age', 'treatment_group', 'risk_group']

    ltbi_person_time = (data[data.cause.str.contains('ltbi')]
                        .drop(columns=drop)
                        .groupby(groupby)
                        .sum()
                        .reset_index())
    ltbi_person_time['cause'] = 'ltbi'

    all_person_time = (data
                       .drop(columns=drop)
                       .groupby(groupby)
                       .sum()
                       .reset_index())
    all_person_time['cause'] = 'all'

    return all_person_time, ltbi_person_time


def sort_data(data: pd.DataFrame) -> pd.DataFrame:
    column_order = ['scenario', 'risk_group', 'treatment_group', 'cause', 'age', 'sex', 'year', 'draw']
    return data.set_index(column_order).sort_index().reset_index()


def split_measures(data: pd.DataFrame, location: str) -> MeasureData:
    deaths = get_measure(data, 'death')
    person_time, ltbi_person_time = get_person_time(data)
    ylls = get_measure(data, 'ylls')
    ylds = get_measure(data, 'ylds')
    tb_cases = get_tb_events(data)
    national_population = get_risk_specific_population(data, location)
    return MeasureData(deaths, person_time, ltbi_person_time, ylls, ylds,
                       tb_cases, national_population)

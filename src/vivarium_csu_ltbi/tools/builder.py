from pathlib import Path
from loguru import logger
import pandas as pd
import numpy as np

from gbd_mapping import causes
from vivarium.framework.artifact import EntityKey, get_location_term, Artifact
from vivarium_inputs.data_artifact.utilities import split_interval
from vivarium_inputs.data_artifact.loaders import loader
from vivarium_inputs import get_measure, utilities, globals, utility_data, get_demographic_dimensions

from vivarium_csu_ltbi import globals as ltbi_globals
from vivarium_csu_ltbi import paths as ltbi_paths


def set_to_known_value(df, set_to):
    for col in df.columns:
        df[col].values[:] = set_to
    return df


class DataRepo:

    def __init__(self):
        self._df_template = None
        self.df_zero = None

    def get_zeros(self):
        return self.df_zero

    def get_filled_with(self, fill_value):
        return pd.DataFrame().reindex_like(self._df_template.copy(deep='all')).fillna(fill_value)

    @staticmethod
    def get_and_package_dismod_ltbi_incidence(loc):
        datafile = ltbi_paths.get_ltbi_inc_input_artifact_path(loc)
        if datafile.exists():
            store = pd.HDFStore(datafile)
            data = store.get('/cause/latent_tuberculosis_infection/incidence')
            data['draw'] = data['draw'].apply(lambda x: f'draw_{x}')
            data.rename(columns={'age_group_start': 'age_start', 'age_group_end': 'age_end'}, inplace=True)
            result = pd.pivot_table(data,
                                    index=['location', 'age_start', 'age_end', 'sex', 'year_start', 'year_end'],
                                    columns='draw', values='value')
            result.columns.name = ''
            return result
        else:
            raise ValueError(f'Error: dismod data "{datafile}" is missing.')

    @staticmethod
    def get_hh_tuberculosis_exposure(loc):
        datafile = ltbi_paths.get_hh_tb_output_artifact_path(loc)
        df = pd.read_hdf(datafile)
        df = df.rename(columns={'age_group_start': 'age_start', 'age_group_end': 'age_end', 'pr_actb_in_hh': 'value'})

        # fix age groups
        young_ages = [(0, 0.01917808), (0.01917808, 0.07671233), (0.07671233, 1)]
        replicated = [df.loc[df.age_start == 0].copy() for _ in
                      range(len(young_ages))]  # create copies of 0-1 age group
        df = df.loc[df.age_start != 0.0]  # remove that group from the data
        for i, (age_start, age_end) in enumerate(young_ages):
            replicated[i].loc[:, 'age_start'] = age_start
            replicated[i].loc[:, 'age_end'] = age_end
        df = pd.concat([df] + replicated, axis=0)

        cat1 = df.copy()
        cat1['parameter'] = 'cat1'
        cat2 = df.copy()
        cat2['parameter'] = 'cat2'
        cat1['value'] = 1 - cat2['value']

        complete = pd.concat([cat1, cat2], axis=0).reset_index(drop=True)
        complete = complete.set_index(['location', 'parameter', 'sex', 'age_start',
                                       'year_start', 'age_end', 'year_end'])
        complete['draw'] = complete['draw'].apply(lambda x: f'draw_{x}')

        wide = pd.pivot_table(complete,
                              index=['location', 'parameter', 'sex', 'age_start', 'year_start', 'age_end', 'year_end'],
                              columns=['draw'], values=['value'])
        wide.columns = wide.columns.get_level_values('draw')
        exposure = utilities.sort_hierarchical_data(wide)

        return exposure

    @staticmethod
    def get_hh_tuberculosis_risk(loc):
        # From Yaqi via Abie. Preliminary, not age- or sex-specific.
        mean = 2.108823418
        ui_lb = 1.488734097
        ui_ub = 2.98719309
        std = (ui_ub - ui_lb) / (2 * 1.96)

        np.random.seed(12221990)
        draws = np.random.normal(mean, std, 1000)

        demog = get_demographic_dimensions(loc)
        demog = split_interval(demog, interval_column='age', split_column_prefix='age')
        demog = split_interval(demog, interval_column='year', split_column_prefix='year').reset_index()
        demog['affected_entity'] = "susceptible_tb_positive_hiv_to_ltbi_positive_hiv"
        demog['affected_measure'] = 'transition_rate'

        cat1 = demog.copy()
        cat1['parameter'] = 'cat1'
        cat1 = pd.concat([cat1, pd.DataFrame(data={f'draw_{i}': [1.0] * len(cat1.index) for i in range(1000)})], axis=1)

        cat2 = demog.copy()
        cat2['parameter'] = 'cat2'
        cat2 = pd.concat([cat2, pd.DataFrame(data={f'draw_{i}': [draws[i]] * len(cat2.index) for i in range(1000)})],
                         axis=1)

        hiv_positive = pd.concat([cat1, cat2], axis=0, ignore_index=True)
        hiv_negative = hiv_positive.copy()
        hiv_negative['affected_entity'] = "susceptible_tb_susceptible_hiv_to_ltbi_susceptible_hiv"

        complete = pd.concat([hiv_negative, hiv_positive], axis=0)
        complete = complete.set_index(['location', 'parameter', 'sex', 'age_start', 'age_end', 'year_start', 'year_end',
                                       'affected_entity', 'affected_measure'])
        rr = utilities.sort_hierarchical_data(complete)

        return rr

    @staticmethod
    def get_hh_tuberculosis_paf(exposure, relative_risk):
        e = exposure.reset_index().set_index(['location', 'parameter', 'sex', 'age_start', 'age_end',
                                              'year_start', 'year_end'])
        r = relative_risk.reset_index().set_index(['location', 'parameter', 'sex', 'age_start',
                                                   'age_end', 'year_start', 'year_end'])

        weighted_rrs = []
        for affected_entity in r.affected_entity.unique():
            ae_rr = r.loc[r.affected_entity == affected_entity]
            affected_measure = list(ae_rr.affected_measure.unique())
            assert len(affected_measure) == 1
            weighted_rr = e * ae_rr
            weighted_rr['affected_entity'] = affected_entity
            weighted_rr['affected_measure'] = affected_measure * len(weighted_rr)
            weighted_rrs.append(weighted_rr)

        weighted_rrs = pd.concat(weighted_rrs, axis=0)
        weighted_rrs = weighted_rrs.set_index(['affected_entity', 'affected_measure'], append=True)
        weighted_rrs = weighted_rrs.loc[pd.notnull(weighted_rrs).all(axis=1)]  # RR has full years, exposure does not
        weighted_rrs = weighted_rrs.droplevel('parameter')

        names = weighted_rrs.index.names
        avg_rr = weighted_rrs.reset_index().groupby(names).sum()  # sums over categories

        paf = (avg_rr - 1) / avg_rr
        paf = utilities.sort_hierarchical_data(paf)

        return paf

    def pull_data(self, loc):
        logger.info('Pulling cause_specific_mortality data')
        self.csmr_297 = get_measure(entity_from_id(297), 'cause_specific_mortality_rate', loc)
        self.csmr_298 = get_measure(entity_from_id(298), 'cause_specific_mortality_rate', loc)
        self.csmr_300 = get_measure(entity_from_id(300), 'cause_specific_mortality_rate', loc)
        self.csmr_934 = get_measure(entity_from_id(934), 'cause_specific_mortality_rate', loc)
        self.csmr_946 = get_measure(entity_from_id(946), 'cause_specific_mortality_rate', loc)
        self.csmr_947 = get_measure(entity_from_id(947), 'cause_specific_mortality_rate', loc)
        self.csmr_948 = get_measure(entity_from_id(948), 'cause_specific_mortality_rate', loc)
        self.csmr_949 = get_measure(entity_from_id(949), 'cause_specific_mortality_rate', loc)
        self.csmr_950 = get_measure(entity_from_id(950), 'cause_specific_mortality_rate', loc)

        logger.info('Pulling incidence_rate data')
        self.i_300 = get_measure(entity_from_id(300), 'incidence_rate', loc)
        self.i_934 = get_measure(entity_from_id(934), 'incidence_rate', loc)
        self.i_946 = get_measure(entity_from_id(946), 'incidence_rate', loc)
        self.i_947 = get_measure(entity_from_id(947), 'incidence_rate', loc)
        self.i_948 = get_measure(entity_from_id(948), 'incidence_rate', loc)
        self.i_949 = get_measure(entity_from_id(949), 'incidence_rate', loc)
        self.i_950 = get_measure(entity_from_id(950), 'incidence_rate', loc)
        self.incidence_ltbi = self.get_and_package_dismod_ltbi_incidence(loc)

        logger.info('Pulling prevalence data')
        self.prev_297 = get_measure(entity_from_id(297), 'prevalence', loc)
        self.prev_298 = get_measure(entity_from_id(298), 'prevalence', loc)
        self.prev_300 = get_measure(entity_from_id(300), 'prevalence', loc)
        self.prev_934 = get_measure(entity_from_id(934), 'prevalence', loc)
        self.prev_946 = get_measure(entity_from_id(946), 'prevalence', loc)
        self.prev_947 = get_measure(entity_from_id(947), 'prevalence', loc)
        self.prev_948 = get_measure(entity_from_id(948), 'prevalence', loc)
        self.prev_949 = get_measure(entity_from_id(949), 'prevalence', loc)
        self.prev_950 = get_measure(entity_from_id(950), 'prevalence', loc)
        self.prev_954 = get_measure(entity_from_id(954), 'prevalence', loc)

        logger.info('Pulling disability weight data')
        self.dw_300 = get_measure(entity_from_id(300), 'disability_weight', loc)
        self.dw_934 = get_measure(entity_from_id(934), 'disability_weight', loc)
        self.dw_946 = get_measure(entity_from_id(946), 'disability_weight', loc)
        self.dw_947 = get_measure(entity_from_id(947), 'disability_weight', loc)
        self.dw_948 = get_measure(entity_from_id(948), 'disability_weight', loc)
        self.dw_949 = get_measure(entity_from_id(949), 'disability_weight', loc)
        self.dw_950 = get_measure(entity_from_id(950), 'disability_weight', loc)

        logger.info('Pulling risk/exposure data')
        self.exposure_hhtb = self.get_hh_tuberculosis_exposure(loc)
        self.risk_hhtb = self.get_hh_tuberculosis_risk(loc)
        self.paf_hhtb = self.get_hh_tuberculosis_paf(self.exposure_hhtb, self.risk_hhtb)

        # TODO: likely a stand-in that will change
        self.dismod_9422_remission = load_em_from_meid(9422, loc)

        # template and zero-filled dataframes
        self._df_template = pd.DataFrame().reindex_like(self.dw_300.copy(deep='all'))
        self.df_zero = self.get_filled_with(0.0)


def entity_from_id(entity_id):
    return [c for c in causes if c.gbd_id == entity_id][0]


def get_load(location):
    return lambda key: loader(EntityKey(key), location, set())


def write_metadata(artifact, location):
    load = get_load(location)
    key = f'cause.{ltbi_globals.TUBERCULOSIS_AND_HIV}.restrictions'
    write(artifact, key, load(f'cause.hiv_aids.restrictions'))


def write_demographic_data(artifact, location, data):
    logger.info('Writing demographic data...')
    load = get_load(location)

    prefix = 'population.'
    measures = ["structure", "age_bins", "theoretical_minimum_risk_life_expectancy", "demographic_dimensions"]
    for m in measures:
        key = prefix + m
        write(artifact, key, load(key))

    key = 'cause.all_causes.cause_specific_mortality_rate'
    write(artifact, key, load(key))

    key = f'cause.{ltbi_globals.TUBERCULOSIS_AND_HIV}.cause_specific_mortality_rate'
    write(artifact, key, (data.csmr_298 + data.csmr_297))


def write_exposure_risk_data(art, data):
    logger.info('In write_exposure_risk_data...')
    write(art, f'risk_factor.{ltbi_globals.HOUSEHOLD_TUBERCULOSIS}.distribution', ltbi_globals.RISK_DISTRIBUTION_TYPE)
    write(art, f'risk_factor.{ltbi_globals.HOUSEHOLD_TUBERCULOSIS}.exposure',
          data.exposure_hhtb, skip_interval_processing=True)
    write(art, f'risk_factor.{ltbi_globals.HOUSEHOLD_TUBERCULOSIS}.relative_risk',
          data.risk_hhtb, skip_interval_processing=True)
    write(art, f'risk_factor.{ltbi_globals.HOUSEHOLD_TUBERCULOSIS}.population_attributable_fraction', data.paf_hhtb,
          skip_interval_processing=True)


def write_baseline_coverage_levels(art, loc):
    import vivarium_csu_ltbi
    data_path = Path(vivarium_csu_ltbi.__file__).parent / 'data'
    logger.info(f'Reading baseline coverage data from {data_path} and writing')

    data = pd.read_csv(data_path / 'baseline_coverage.csv')
    data = data.rename(columns={'year': 'year_start'})
    data['year_end'] = data['year_start'] + 1
    data = data.set_index(['location'])
    data['value'] /= 100.

    demog = get_demographic_dimensions(loc)
    demog = split_interval(demog, interval_column='age', split_column_prefix='age')
    demog = demog.reset_index().drop(['year'], axis=1)
    demog = demog.drop_duplicates()
    demog = demog.set_index(['location'])

    duplicated = pd.merge(demog, data, left_index=True, right_index=True)

    six_h = duplicated.copy()
    six_h = six_h.set_index(['sex', 'age_start', 'age_end', 'year_start', 'year_end', 'treatment_subgroup'],
                            append=True)

    three_hp = duplicated.copy()
    three_hp['value'] = 0.0
    three_hp = three_hp.set_index(['sex', 'age_start', 'age_end', 'year_start', 'year_end', 'treatment_subgroup'],
                                  append=True)

    six_h = pd.DataFrame(data={f'draw_{i}': six_h['value'] for i in range(1000)}, index=six_h.index)
    three_hp = pd.DataFrame(data={f'draw_{i}': three_hp['value'] for i in range(1000)}, index=three_hp.index)

    write(art, 'ltbi_treatment.six_h.coverage', six_h, skip_interval_processing=True)
    write(art, 'ltbi_treatment.three_hp.coverage', three_hp, skip_interval_processing=True)


def sample_from_normal(mean, std, index_name):
    draw = np.random.normal(mean, std, size=1000)
    return pd.DataFrame(data={f'draw_{i}': draw[i] for i in range(1000)},
                        index=pd.Index([index_name], name='treatment_type'))


def write_adherence_data(art, location):
    import vivarium_csu_ltbi
    data_path = Path(vivarium_csu_ltbi.__file__).parent / 'data'
    logger.info(f"Reading adherence from {data_path}")
    data = pd.read_csv(data_path / "treatment_adherence_draws.csv", usecols=['adherence_3hp_real_world',
                                                                             'adherence_6h_real_world'])

    three_hp_adherence = data['adherence_3hp_real_world']
    three_hp_adherence = pd.DataFrame(data={f'draw_{i}': three_hp_adherence.iloc[i] for i in range(1000)},
                                      index=pd.Index(['3HP'], name='treatment_type'))

    six_h_adherence = data['adherence_6h_real_world']
    six_h_adherence = pd.DataFrame(data={f'draw_{i}': six_h_adherence.iloc[i] for i in range(1000)},
                                   index=pd.Index(['6H'], name='treatment_type'))

    demog = get_demographic_dimensions(location)
    demog = split_interval(demog, interval_column='age', split_column_prefix='age')
    demog = split_interval(demog, interval_column='year', split_column_prefix='year')
    demog['treatment_type'] = '3HP'
    three_hp_index = demog.set_index('treatment_type', append=True)
    three_hp_data = three_hp_index.join(three_hp_adherence)

    demog['treatment_type'] = '6H'
    six_h_index = demog.set_index('treatment_type', append=True)
    six_h_data = six_h_index.join(six_h_adherence)

    adherence_data = pd.concat([three_hp_data, six_h_data], axis=0)

    write(art, 'ltbi_treatment.adherence', adherence_data, skip_interval_processing=True)


def write_relative_risk_data(art, location):
    import vivarium_csu_ltbi
    data_path = Path(vivarium_csu_ltbi.__file__).parent / 'data'
    logger.info(f"Reading relative risk data from {data_path}")
    data = pd.read_csv(data_path / "treatment_adherence_draws.csv", usecols=['RR_no_tx',
                                                                             'RR_NA'])

    untreated_relative_risk = data['RR_no_tx']
    untreated_rr = pd.DataFrame(data={f'draw_{i}': untreated_relative_risk.iloc[i] for i in range(1000)},
                                index=pd.Index(['untreated'], name='parameter'))

    # NOTE: Currently, each relative risk is the same for each drug
    nonadherent_relative_risk = data['RR_NA']
    nonadherent_six_h_rr = pd.DataFrame(data={f'draw_{i}': nonadherent_relative_risk.iloc[i] for i in range(1000)},
                                        index=pd.Index(['6H_nonadherent'], name='parameter'))
    nonadherent_three_hp_rr = pd.DataFrame(data={f'draw_{i}': nonadherent_relative_risk.iloc[i] for i in range(1000)},
                                           index=pd.Index(['3HP_nonadherent'], name='parameter'))

    adherent_relative_risk = 1.0
    adherent_six_h_rr = pd.DataFrame(data={f'draw_{i}': adherent_relative_risk for i in range(1000)},
                                     index=pd.Index(['6H_adherent'], name='parameter'))
    adherent_three_hp_rr = pd.DataFrame(data={f'draw_{i}': adherent_relative_risk for i in range(1000)},
                                        index=pd.Index(['3HP_adherent'], name='parameter'))

    demog = get_demographic_dimensions(location)
    demog = split_interval(demog, interval_column='age', split_column_prefix='age')
    demog = split_interval(demog, interval_column='year', split_column_prefix='year')

    individual_rr_data = {
        'untreated': untreated_rr,
        '6H_nonadherent': nonadherent_six_h_rr,
        '3HP_nonadherent': nonadherent_three_hp_rr,
        '6H_adherent': adherent_six_h_rr,
        '3HP_adherent': adherent_three_hp_rr
    }

    full_cat_data = []
    for cat in ['untreated', '6H_nonadherent', '3HP_nonadherent', '6H_adherent', '3HP_adherent']:
        demog['parameter'] = cat
        data_demog = demog.set_index('parameter', append=True)
        data = data_demog.join(individual_rr_data[cat])
        full_cat_data.append(data)

    full_cat_data = pd.concat(full_cat_data, axis=0)
    full_cat_data['affected_entity'] = 'tuberculosis_and_hiv'

    affects_hiv_pos = full_cat_data.copy()
    affects_hiv_pos['affected_measure'] = 'ltbi_positive_hiv_to_activetb_positive_hiv'

    affects_hiv_neg = full_cat_data.copy()
    affects_hiv_neg['affected_measure'] = 'ltbi_susceptible_hiv_to_activetb_susceptible_hiv'

    full_rr_data = pd.concat([affects_hiv_pos, affects_hiv_neg], axis=0)
    full_rr_data = full_rr_data.set_index(['affected_entity', 'affected_measure'], append=True)

    write(art, 'ltbi_treatment.relative_risk', full_rr_data, skip_interval_processing=True)


def compute_prevalence(art, data):
    logger.info('Computing prevalence...')

    state_sus_tb_sus_hiv = 1 - ((data.prev_297 + data.prev_298) - (data.prev_954 * data.prev_300))
    write(art, f'sequela.{ltbi_globals.SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV}.prevalence', state_sus_tb_sus_hiv)

    state_df_ltbi_s_hiv = data.prev_954 * (1 - data.prev_300)
    write(art, f'sequela.{ltbi_globals.LTBI_SUSCEPTIBLE_HIV}.prevalence', state_df_ltbi_s_hiv)

    state_act_tb_sus_hiv = data.prev_934 + data.prev_946 + data.prev_947
    write(art, f'sequela.{ltbi_globals.ACTIVETB_SUSCEPTIBLE_HIV}.prevalence', state_act_tb_sus_hiv)

    state_sus_tb_hiv_plus = (1 - data.prev_954) * data.prev_300
    write(art, f'sequela.{ltbi_globals.SUSCEPTIBLE_TB_POSITIVE_HIV}.prevalence', state_sus_tb_hiv_plus)

    state_ltbi_hiv_plus = data.prev_954 * data.prev_300
    write(art, f'sequela.{ltbi_globals.LTBI_POSITIVE_HIV}.prevalence', state_ltbi_hiv_plus)

    state_act_tb_hiv_plus = data.prev_948 + data.prev_949 + data.prev_950
    write(art, f'sequela.{ltbi_globals.ACTIVETB_POSITIVE_HIV}.prevalence', state_act_tb_hiv_plus)


def compute_excess_mortality(art, data):
    logger.info('Computing excess_mortality...')

    write(art, f'sequela.{ltbi_globals.SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV}.excess_mortality_rate', data.get_zeros())

    state_act_tb_sus_hiv = ((data.csmr_934 + data.csmr_946 + data.csmr_947)
                           / (data.prev_934 + data.prev_946 + data.prev_947))
    write(art, f'sequela.{ltbi_globals.ACTIVETB_SUSCEPTIBLE_HIV}.excess_mortality_rate', state_act_tb_sus_hiv)

    emr_300 = data.csmr_300 / data.prev_300
    write(art, f'sequela.{ltbi_globals.SUSCEPTIBLE_TB_POSITIVE_HIV}.excess_mortality_rate', emr_300)
    write(art, f'sequela.{ltbi_globals.LTBI_SUSCEPTIBLE_HIV}.excess_mortality_rate', data.get_zeros())
    write(art, f'sequela.{ltbi_globals.LTBI_POSITIVE_HIV}.excess_mortality_rate', emr_300)

    state_act_tb_plus_hiv = ((data.csmr_948 + data.csmr_949 + data.csmr_950)
                            / (data.prev_948 + data.prev_949 + data.prev_950))
    write(art, f'sequela.{ltbi_globals.ACTIVETB_POSITIVE_HIV}.excess_mortality_rate', state_act_tb_plus_hiv)


def get_total_disability_weight(prevs, weights):
    total_prevalence = sum(prevs)
    total_disability_weight = sum([p * dw for p, dw in zip(prevs, weights)]) / total_prevalence
    return total_disability_weight


def compute_disability_weight(art, data):
    logger.info('Computing disability_weight...')

    write(art, f'sequela.{ltbi_globals.SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV}.disability_weight', data.get_zeros())
    write(art, f'sequela.{ltbi_globals.LTBI_SUSCEPTIBLE_HIV}.disability_weight', data.get_zeros())

    total_disability_weight = get_total_disability_weight(
        [data.prev_934, data.prev_946, data.prev_947], [data.dw_934, data.dw_946, data.dw_947])

    write(art, f'sequela.{ltbi_globals.ACTIVETB_SUSCEPTIBLE_HIV}.disability_weight', total_disability_weight)
    write(art, f'sequela.{ltbi_globals.SUSCEPTIBLE_TB_POSITIVE_HIV}.disability_weight', data.dw_300)
    write(art, f'sequela.{ltbi_globals.LTBI_POSITIVE_HIV}.disability_weight', data.dw_300)

    total_disability_weight = get_total_disability_weight(
        [data.prev_948, data.prev_949, data.prev_950], [data.dw_948, data.dw_949, data.dw_950])

    write(art, f'sequela.{ltbi_globals.ACTIVETB_POSITIVE_HIV}.disability_weight', total_disability_weight)


def load_em_from_meid(meid, location):
    from vivarium_gbd_access import gbd
    location_id = utility_data.get_location_id(location)
    data = gbd.get_modelable_entity_draws(meid, location_id)
    data = data[data.measure_id == globals.MEASURES['Remission rate']]
    data = utilities.normalize(data, fill_value=0)
    data = data.filter(globals.DEMOGRAPHIC_COLUMNS + globals.DRAW_COLUMNS)
    data = utilities.reshape(data)
    data = utilities.scrub_gbd_conventions(data, location)
    return utilities.sort_hierarchical_data(data)


def compute_transition_rates(art, data):
    logger.info('Computing transition_rates...')

    write(art, f'sequela.{ltbi_globals.SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV_TO_LTBI_SUSCEPTIBLE_HIV}.transition_rate',
          data.incidence_ltbi, skip_interval_processing=True)
    write(art, f'sequela.{ltbi_globals.SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV_TO_SUSCEPTIBLE_TB_POSITIVE_HIV}.transition_rate',
          data.i_300)
    write(art, f'sequela.{ltbi_globals.LTBI_SUSCEPTIBLE_HIV_TO_ACTIVETB_SUSCEPTIBLE_HIV}.transition_rate',
          (data.i_934 + data.i_946 + data.i_947) / (data.prev_954 * (1 - data.prev_300)))
    write(art, f'sequela.{ltbi_globals.LTBI_SUSCEPTIBLE_HIV_TO_LTBI_POSITIVE_HIV}.transition_rate',
          data.i_300)
    write(art, f'sequela.{ltbi_globals.SUSCEPTIBLE_TB_POSITIVE_HIV_TO_LTBI_POSITIVE_HIV}.transition_rate',
          data.incidence_ltbi, skip_interval_processing=True)
    write(art, f'sequela.{ltbi_globals.LTBI_POSITIVE_HIV_TO_ACTIVETB_POSITIVE_HIV}.transition_rate',
          (data.i_948 + data.i_949 + data.i_950) / (data.prev_954 * data.prev_300))
    write(art, f'sequela.{ltbi_globals.ACTIVETB_POSITIVE_HIV_TO_SUSCEPTIBLE_TB_POSITIVE_HIV}.transition_rate',
          data.dismod_9422_remission)
    write(art, f'sequela.{ltbi_globals.ACTIVETB_SUSCEPTIBLE_HIV_TO_ACTIVETB_POSITIVE_HIV}.transition_rate',
          data.i_300)
    write(art, f'sequela.{ltbi_globals.ACTIVETB_SUSCEPTIBLE_HIV_TO_SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV}.transition_rate',
          data.dismod_9422_remission)


def create_new_artifact(path: str, location: str) -> Artifact:
    if Path(path).is_file():
        Path(path).unlink()
    art = Artifact(path, filter_terms=[get_location_term(location)])
    art.write('metadata.locations', [location])
    return art


def write(artifact, key, data, skip_interval_processing=False):
    if skip_interval_processing:
        tmp = data
    else:
        tmp = data.copy(deep='all') if isinstance(data, pd.core.frame.DataFrame) else data
        tmp = split_interval(tmp, interval_column='age', split_column_prefix='age')
        tmp = split_interval(tmp, interval_column='year', split_column_prefix='year')
    if isinstance(tmp, pd.core.frame.DataFrame):
        assert 'age_end' in tmp.index.names
    artifact.write(key, tmp)


def build_ltbi_artifact(loc, output_dir=None):
    data = DataRepo()
    data.pull_data(loc)
    out_path = f'{loc.replace(" ",  "_").lower()}.hdf' if output_dir else ltbi_paths.get_final_artifact_path(loc)
    art = create_new_artifact(out_path, loc)
    write_demographic_data(art, loc, data)
    write_metadata(art, loc)

    compute_prevalence(art, data)
    compute_excess_mortality(art, data)
    compute_disability_weight(art, data)
    compute_transition_rates(art, data)
    write_exposure_risk_data(art, data)
    write_baseline_coverage_levels(art, loc)
    write_adherence_data(art, loc)
    write_relative_risk_data(art, loc)

    logger.info('!!! Done !!!')

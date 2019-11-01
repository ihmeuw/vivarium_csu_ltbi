from pathlib import Path
from loguru import logger
import pandas as pd

from gbd_mapping import causes
from vivarium.framework.artifact import EntityKey, get_location_term, Artifact
from vivarium_inputs.data_artifact.utilities import split_interval
from vivarium_inputs.data_artifact.loaders import loader
from vivarium_inputs import get_measure, utilities, globals, utility_data
from vivarium_gbd_access import gbd
from ..components.names import *


PROJ_NAME = 'vivarium_csu_ltbi'
DEFAULT_PATH = gbd.ARTIFACT_FOLDER / PROJ_NAME


class DataRepo:

    def __init__(self):
        self._df_template = None
        self.df_zero = None

    def get_zeros(self):
        return self.df_zero

    def get_filled_with(self, fill_value):
        return pd.DataFrame().reindex_like(self._df_template.copy(deep='all')).fillna(fill_value)


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
        # vivarium_inputs.globals.InvalidQueryError: Deaths data is not expected to exist for cause
        #  latent_tuberculosis_infection.
        #self.csmr_954 = get_measure(entity_from_id(954), 'cause_specific_mortality', loc)

        # self.emr_300 = get_measure(entity_from_id(), '', loc)

        logger.info('Pulling incidence_rate data')
        self.i_300 = get_measure(entity_from_id(300), 'incidence_rate', loc)
        self.i_934 = get_measure(entity_from_id(934), 'incidence_rate', loc)
        self.i_946 = get_measure(entity_from_id(946), 'incidence_rate', loc)
        self.i_947 = get_measure(entity_from_id(947), 'incidence_rate', loc)
        self.i_948 = get_measure(entity_from_id(948), 'incidence_rate', loc)
        self.i_949 = get_measure(entity_from_id(949), 'incidence_rate', loc)
        self.i_950 = get_measure(entity_from_id(950), 'incidence_rate', loc)
        # DataDoesNotExistError: Data contains no non-missing, non-zero values.
        #self.i_954 = get_measure(entity_from_id(954), 'incidence', loc)

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

        # likely a stand-in that will change
        self.dismod_9422_remission = load_em_from_meid(9422, loc)

        # template and zero-filled dataframes
        self._df_template = pd.DataFrame().reindex_like(self.dw_300.copy(deep='all'))
        self.df_zero = self.get_filled_with(0.0)
        logger.info(f'Zero DF looks like  = {self.df_zero.head()}')

        #self.sequelae_300 = get_measure(entity_from_id(300), '', loc)

        #self.ylds_297 = get_measure(entity_from_id(), '', loc)
        #self.ylds_298 = get_measure(entity_from_id(), '', loc)


def entity_from_id(id):
    return [c for c in causes if c.gbd_id == id][0]


def get_load(location):
    return lambda key: loader(EntityKey(key), location, set())


def write_metadata(artifact, location):
    load = get_load(location)
    key = f'cause.{LATENT_TUBERCULOSIS_INFECTION}.sequelae'
    write(artifact, key, load(key))
    key = f'cause.{LATENT_TUBERCULOSIS_INFECTION}.restrictions'
    write(artifact, key, load(key))


def write_demographic_data(artifact, location):
    logger.info('Writing demographic data...')
    load = get_load(location)

    prefix = 'population.'
    measures = ["structure", "age_bins", "theoretical_minimum_risk_life_expectancy", "demographic_dimensions"]
    for m in measures:
        key = prefix + m
        write(artifact, key, load(key))

    key = 'cause.all_causes.cause_specific_mortality_rate'
    write(artifact, key, load(key))


def compute_prevalence(art, data):
    logger.info('Computing prevalence...')

    state_sus_tb_sus_hiv = 1 - ((data.prev_297 + data.prev_298) - (data.prev_954 * data.prev_300))
    write(art, f'sequela.{SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV}.prevalence', state_sus_tb_sus_hiv)

    state_df_ltbi_s_hiv = data.prev_954 * (1 - data.prev_300)
    write(art, f'sequela.{LTBI_SUSCEPTIBLE_HIV}.prevalence', state_df_ltbi_s_hiv)

    state_act_tb_sus_hiv = data.prev_934 + data.prev_946 + data.prev_947
    write(art, f'sequela.{ACTIVETB_SUSCEPTIBLE_HIV}.prevalence', state_act_tb_sus_hiv)

    write(art, f'sequela.{RECOVERED_LTBI_SUSCEPTIBLE_HIV}.prevalence', data.get_zeros())
    write(art, f'sequela.{RECOVERED_LTBI_POSITIVE_HIV}.prevalence', data.get_zeros())

    state_sus_tb_hiv_plus = (1 - data.prev_954) * data.prev_300
    write(art, f'sequela.{SUSCEPTIBLE_TB_POSITIVE_HIV}.prevalence', state_sus_tb_hiv_plus)

    state_ltbi_hiv_plus = data.prev_954 * data.prev_300
    write(art, f'sequela.{LTBI_POSITIVE_HIV}.prevalence', state_ltbi_hiv_plus)
    
    state_act_tb_hiv_plus = data.prev_948 + data.prev_949 + data.prev_950
    write(art, f'sequela.{ACTIVETB_POSITIVE_HIV}.prevalence', state_act_tb_hiv_plus)


def compute_excess_mortality(art, data):
    logger.info('Computing excess_mortality...')

    state_act_tb_sus_hiv = ((data.csmr_934 + data.csmr_946 + data.csmr_947)
                           / (data.prev_934 + data.prev_946 + data.prev_947))
    write(art, f'sequela.{ACTIVETB_SUSCEPTIBLE_HIV}.excess_mortality', state_act_tb_sus_hiv)

    emr_300 = data.csmr_300 / data.prev_300
    write(art, f'sequela.{RECOVERED_LTBI_POSITIVE_HIV}.excess_mortality', emr_300)
    write(art, f'sequela.{SUSCEPTIBLE_TB_POSITIVE_HIV}.excess_mortality', emr_300)
    write(art, f'sequela.{LTBI_POSITIVE_HIV}.excess_mortality', emr_300)

    state_act_tb_plus_hiv = ((data.csmr_948 + data.csmr_949 + data.csmr_950)
                            / (data.prev_948 + data.prev_949 + data.prev_950))
    write(art, f'sequela.{ACTIVETB_POSITIVE_HIV}.excess_mortality', state_act_tb_plus_hiv)


def get_total_disability_weight(prevs, weights):
    total_prevalence = sum(prevs)
    total_disability_weight = sum([p * dw for p, dw in zip(prevs, weights)]) / total_prevalence
    return total_disability_weight


def compute_disability_weight(art, data):
    logger.info('Computing disability_weight...')

    write(art, f'sequela.{SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV}.disability_weight', data.get_zeros())
    write(art, f'sequela.{LTBI_SUSCEPTIBLE_HIV}.disability_weight', data.get_zeros())

    total_disability_weight = get_total_disability_weight(
        [data.prev_934, data.prev_946, data.prev_947], [data.dw_934, data.dw_946, data.dw_947])
    logger.info(f'Disability wt. = {total_disability_weight.head()}')

    write(art, f'sequela.{ACTIVETB_SUSCEPTIBLE_HIV}.disability_weight', total_disability_weight)
    write(art, f'sequela.{RECOVERED_LTBI_SUSCEPTIBLE_HIV}.disability_weight', data.dw_300)
    write(art, f'sequela.{RECOVERED_LTBI_POSITIVE_HIV}.disability_weight', data.dw_300)
    write(art, f'sequela.{SUSCEPTIBLE_TB_POSITIVE_HIV}.disability_weight', data.dw_300)
    write(art, f'sequela.{LTBI_POSITIVE_HIV}.disability_weight', data.dw_300)

    total_disability_weight = get_total_disability_weight(
        [data.prev_948, data.prev_949, data.prev_950], [data.dw_948, data.dw_949, data.dw_950])
    logger.info(f'Disability wt. = {total_disability_weight.head()}')

    write(art, f'sequela.{ACTIVETB_POSITIVE_HIV}.disability_weight', total_disability_weight)


def load_em_from_meid(meid, location):
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

    # TODO - names acceptable???
    write(art, f'sequela.{SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV_TO_LTBI_SUSCEPTIBLE_HIV}.transition_rate',
          data.get_filled_with(0.01))
    write(art, f'sequela.{SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV_TO_SUSCEPTIBLE_TB_POSITIVE_HIV}.transition_rate',
          data.i_300)
    write(art, f'sequela.{LTBI_SUSCEPTIBLE_HIV_TO_RECOVERED_LTBI_SUSCEPTIBLE_HIV}.transition_rate',
          data.get_zeros())
    write(art, f'sequela.{LTBI_SUSCEPTIBLE_HIV_TO_ACTIVETB_SUSCEPTIBLE_HIV}.transition_rate',
          (data.i_934 + data.i_946 + data.i_947) / (data.prev_954 * (1 - data.prev_300)))
    write(art, f'sequela.{LTBI_SUSCEPTIBLE_HIV_TO_LTBI_POSITIVE_HIV}.transition_rate',
          data.i_300)
    write(art, f'sequela.{RECOVERED_LTBI_SUSCEPTIBLE_HIV_TO_SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV}.transition_rate',
          data.get_zeros())
    write(art, f'sequela.{RECOVERED_LTBI_SUSCEPTIBLE_HIV_TO_RECOVERED_LTBI_POSITIVE_HIV}.transition_rate',
          data.i_300)
    write(art, f'sequela.{RECOVERED_LTBI_POSITIVE_HIV_TO_SUSCEPTIBLE_TB_POSITIVE_HIV}.transition_rate',
          data.get_zeros())
    write(art, f'sequela.{SUSCEPTIBLE_TB_POSITIVE_HIV_TO_LTBI_POSITIVE_HIV}.transition_rate',
          data.get_filled_with(0.01))
    write(art, f'sequela.{LTBI_POSITIVE_HIV_TO_RECOVERED_LTBI_POSITIVE_HIV}.transition_rate',
          data.get_zeros())
    write(art, f'sequela.{LTBI_POSITIVE_HIV_ACTIVETB_POSITIVE_HIV}.transition_rate',
          (data.i_948 + data.i_949 + data.i_950) / (data.prev_954 * data.prev_300))
    write(art, f'sequela.{ACTIVETB_POSITIVE_HIV_TO_SUSCEPTIBLE_TB_POSITIVE_HIV}.transition_rate',
          data.dismod_9422_remission)
    write(art, f'sequela.{ACTIVETB_SUSCEPTIBLE_HIV_TO_ACTIVETB_POSITIVE_HIV}.transition_rate',
          data.i_300)
    write(art, f'sequela.{ACTIVETB_SUSCEPTIBLE_HIV_SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV}.transition_rate',
          data.dismod_9422_remission)


def create_new_artifact(path: str, location: str) -> Artifact:
    if Path(path).is_file():
        Path(path).unlink()
    art = Artifact(path, filter_terms=[get_location_term(location)])
    art.write('metadata.locations', [location])
    return art


def write(artifact, key, data):
    tmp = data.copy(deep='all') if isinstance(data, pd.core.frame.DataFrame) else data
    tmp = split_interval(tmp, interval_column='age', split_column_prefix='age')
    tmp = split_interval(tmp, interval_column='year', split_column_prefix='year')
    if isinstance(tmp, pd.core.frame.DataFrame):
        assert 'age_end' in tmp.index.names
    artifact.write(key, tmp)


def build_ltbi_artifact(loc, output_dir=None):
    data = DataRepo()
    data.pull_data(loc)
    out_path = f'{output_dir}/{loc}.hdf' if output_dir else f'{DEFAULT_PATH}/{loc}.hdf'
    art = create_new_artifact(out_path, loc)
    write_demographic_data(art, loc)
    write_metadata(art, loc)
    compute_prevalence(art, data)
    compute_excess_mortality(art, data)
    compute_disability_weight(art, data)
    compute_transition_rates(art, data)
    logger.info('!!! Done !!!')









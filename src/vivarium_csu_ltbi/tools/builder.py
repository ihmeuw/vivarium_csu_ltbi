from pathlib import Path
from loguru import logger


from gbd_mapping import causes
from vivarium_public_health.dataset_manager import Artifact, get_location_term
from vivarium_inputs.data_artifact.utilities import split_interval
from vivarium_inputs import get_measure
from vivarium_gbd_access.gbd import ARTIFACT_FOLDER


PROJ_NAME = 'vivarium_csu_ltbi'
DEFAULT_PATH = ARTIFACT_FOLDER / PROJ_NAME

cause_latent_tuberculosis_infection_954 = 954
cause_drug_sus_tb_934 = 934
cause_m_drug_res_tb_946 = 946
cause_ext_d_res_tb_947 = 947
hiv_d_sus_tb_948 = 948
hiv_m_d_res_tb_949 = 949
hiv_ext_d_res_tb_950 = 950
hiv_all_other_300 = 300
tb_297 = 297
hiv_298 = 298

#  Prevalence state names
SUS_TB_SUS_HIV = 'sus_tb_sus_hiv'
LTBI_SUS_HIV = 'ltbi_sus_hiv'
ACTTB_SUS_HIV = 'acttb_sus_hiv'
REC_LTBI_SUS_HIV = 'rec_ltbi_sus_hiv'
REC_LTBI_PLUS_HIV = 'rec_ltbi_plus_hiv'
SUS_TB_PLUS_HIV = 'sus_tb_plus_hiv'
LTBI_PLUS_HIV = 'ltbi_plus_hiv'
ACTTB_PLUS_HIV = 'acttb_plus_hiv'


class DataRepo:

    def __init__(self):
        pass

    def pull_data(self, loc):
        logger.info('Pulling cause_specific_mortality data')
        self.csmr_297 = get_measure(entity_from_id(297), 'cause_specific_mortality', loc)
        self.csmr_298 = get_measure(entity_from_id(298), 'cause_specific_mortality', loc)
        self.csmr_300 = get_measure(entity_from_id(300), 'cause_specific_mortality', loc)
        self.csmr_934 = get_measure(entity_from_id(934), 'cause_specific_mortality', loc)
        self.csmr_946 = get_measure(entity_from_id(946), 'cause_specific_mortality', loc)
        self.csmr_947 = get_measure(entity_from_id(947), 'cause_specific_mortality', loc)
        self.csmr_948 = get_measure(entity_from_id(948), 'cause_specific_mortality', loc)
        self.csmr_949 = get_measure(entity_from_id(949), 'cause_specific_mortality', loc)
        self.csmr_950 = get_measure(entity_from_id(950), 'cause_specific_mortality', loc)
        # vivarium_inputs.globals.InvalidQueryError: Deaths data is not expected to exist for cause
        #  latent_tuberculosis_infection.
        #self.csmr_954 = get_measure(entity_from_id(954), 'cause_specific_mortality', loc)

        # self.emr_300 = get_measure(entity_from_id(), '', loc)

        logger.info('Pulling incidence data')
        self.i_300 = get_measure(entity_from_id(300), 'incidence', loc)
        self.i_934 = get_measure(entity_from_id(934), 'incidence', loc)
        self.i_946 = get_measure(entity_from_id(946), 'incidence', loc)
        self.i_947 = get_measure(entity_from_id(947), 'incidence', loc)
        self.i_948 = get_measure(entity_from_id(948), 'incidence', loc)
        self.i_949 = get_measure(entity_from_id(949), 'incidence', loc)
        self.i_950 = get_measure(entity_from_id(950), 'incidence', loc)
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

        #self.sequelae_300 = get_measure(entity_from_id(300), '', loc)

        #self.ylds_297 = get_measure(entity_from_id(), '', loc)
        #self.ylds_298 = get_measure(entity_from_id(), '', loc)


def entity_from_id(id):
    return [c for c in causes if c.gbd_id == id][0]


def compute_prevalence(art, data):
    logger.info('Computing prevalence...')

    state_sus_tb_sus_hiv = 1 - ((data.prev_297 + data.prev_298) - (data.prev_954 * data.prev_300))
    write(art, f'state.{SUS_TB_SUS_HIV}.prevalence', state_sus_tb_sus_hiv)

    state_df_ltbi_s_hiv = data.prev_954 * (1 - data.prev_300)
    write(art, f'state.{LTBI_SUS_HIV}.prevalence', state_df_ltbi_s_hiv)

    state_act_tb_sus_hiv = data.prev_934 + data.prev_946 + data.prev_947
    write(art, f'state.{ACTTB_SUS_HIV}.prevalence', state_act_tb_sus_hiv)

    state_sus_tb_hiv_plus = (1 - data.prev_954) * data.prev_300
    write(art, f'state.{SUS_TB_PLUS_HIV}.prevalence', state_sus_tb_hiv_plus)

    state_ltbi_hiv_plus = data.prev_954 * data.prev_300
    write(art, f'state.{LTBI_PLUS_HIV}.prevalence', state_ltbi_hiv_plus)
    
    state_act_tb_hiv_plus = data.prev_948 + data.prev_949 + data.prev_950
    write(art, f'state.{ACTTB_PLUS_HIV}.prevalence', state_act_tb_hiv_plus)


def compute_excess_mortality(art, data):
    logger.info('Computing excess_mortality...')

    state_act_tb_sus_hiv = ((data.csmr_934 + data.csmr_946 + data.csmr_947)
                           / (data.prev_934 + data.prev_946 + data.prev_947))
    write(art, f'state.{ACTTB_SUS_HIV}.excess_mortality', state_act_tb_sus_hiv)

    emr_300 = data.csmr_300 / data.prev_300
    write(art, f'state.{REC_LTBI_PLUS_HIV}.excess_mortality', emr_300)
    write(art, f'state.{SUS_TB_PLUS_HIV}.excess_mortality', emr_300)
    write(art, f'state.{LTBI_PLUS_HIV}.excess_mortality', emr_300)

    state_act_tb_plus_hiv = ((data.csmr_948 + data.csmr_949 + data.csmr_950)
                            / (data.prev_948 + data.prev_949 + data.prev_950))
    write(art, f'state.{ACTTB_PLUS_HIV}.excess_mortality', state_act_tb_plus_hiv)


def create_new_artifact(path: str, location: str) -> Artifact:
    if Path(path).is_file():
        Path(path).unlink()
    art = Artifact(path, filter_terms=[get_location_term(location)])
    art.write('metadata.locations', [location])
    return art


def write(artifact, key, data):
    data = split_interval(data, interval_column='age', split_column_prefix='age')
    data = split_interval(data, interval_column='year', split_column_prefix='year')
    artifact.write(key, data)


def build_artifact(loc):
    data = DataRepo()
    data.pull_data(loc)
    art = create_new_artifact(f'{DEFAULT_PATH}/{loc}.hdf', loc)
    #art = create_new_artifact(f'/ihme/homes/kjells/artifacts/{loc}.hdf', loc)
    compute_prevalence(art, data)
    compute_excess_mortality(art, data)

build_artifact('Ethiopia')









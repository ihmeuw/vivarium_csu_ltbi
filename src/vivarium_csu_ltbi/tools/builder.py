from pathlib import Path
from loguru import logger


from gbd_mapping import causes
from vivarium_public_health.dataset_manager import Artifact, EntityKey, get_location_term
from vivarium_inputs.data_artifact.utilities import split_interval
from vivarium_inputs import get_measure, utilities, globals, utility_data, core
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


def entity_from_id(id):
    return [c for c in causes if c.gbd_id == id][0]

def get_prevalence_data(art, loc):
    df_954 = get_measure(entity_from_id(cause_latent_tuberculosis_infection_954), 'prevalence', loc)
    df_300 = get_measure(entity_from_id(hiv_all_other_300), 'prevalence', loc)
    state_df_ltbi_s_hiv = df_954 * (1 - df_300)
    write(art, f'state.{LTBI_SUS_HIV}.prevalence', state_df_ltbi_s_hiv)

    df_934 = get_measure(entity_from_id(cause_drug_sus_tb_934), 'prevalence', loc)
    df_946 = get_measure(entity_from_id(cause_m_drug_res_tb_946), 'prevalence', loc)
    df_947 = get_measure(entity_from_id(cause_ext_d_res_tb_947), 'prevalence', loc)
    state_act_tb_sus_hiv = df_934 + df_946 + df_947
    write(art, f'state.{ACTTB_SUS_HIV}.prevalence', state_act_tb_sus_hiv)

    state_sus_tb_hiv_plus = (1 - df_954) * df_300
    write(art, f'state.{SUS_TB_PLUS_HIV}.prevalence', state_act_tb_sus_hiv)

    state_ltbi_hiv_plus = df_954 * df_300
    write(art, f'state.{LTBI_PLUS_HIV}.prevalence', state_act_tb_sus_hiv)

    df_948 = get_measure(entity_from_id(hiv_d_sus_tb_948), 'prevalence', loc)
    df_949 = get_measure(entity_from_id(hiv_m_d_res_tb_949), 'prevalence', loc)
    df_950 = get_measure(entity_from_id(hiv_ext_d_res_tb_950), 'prevalence', loc)
    state_act_tb_hiv_plus = df_948 + df_949 + df_950
    write(art, f'state.{ACTTB_PLUS_HIV}.prevalence', state_act_tb_sus_hiv)

    df_297 = get_measure(entity_from_id(tb_297), 'prevalence', loc)
    df_298 = get_measure(entity_from_id(hiv_298), 'prevalence', loc)
    state_sus_tb_sus_hiv = 1 - ((df_297 + df_298) - (df_954 * df_300))
    write(art, f'state.{SUS_TB_SUS_HIV}.prevalence', state_sus_tb_sus_hiv)


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
    art = create_new_artifact(f'{DEFAULT_PATH}/kjells.hdf', 'Ethiopia')
    get_prevalence_data(art, 'Ethiopia')


build_artifact('Ethiopia')









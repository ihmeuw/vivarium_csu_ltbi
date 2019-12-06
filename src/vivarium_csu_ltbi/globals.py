"""Global variables for ltbi project."""
from pathlib import Path


# disease name
TUBERCULOSIS_AND_HIV = 'tuberculosis_and_hiv'

# risk names
HOUSEHOLD_TUBERCULOSIS = 'household_tuberculosis'
RISK_DISTRIBUTION_TYPE = 'dichotomous'
HOUSEHOLD_TUBERCULOSIS_EXPOSED = 'exposed_to_hhtb'
HOUSEHOLD_TUBERCULOSIS_UNEXPOSED = 'unexposed_to_hhtb'
HOUSEHOLD_TUBERCULOSIS_EXPOSURE_CATEGORIES = ['cat1', 'cat2']
HOUSEHOLD_TUBERCULOSIS_EXPOSURE_MAP = {'cat1': HOUSEHOLD_TUBERCULOSIS_EXPOSED,
                                       'cat2': HOUSEHOLD_TUBERCULOSIS_UNEXPOSED}

#  state names
ACTIVETB_POSITIVE_HIV = 'activetb_positive_hiv'
ACTIVETB_SUSCEPTIBLE_HIV = 'activetb_susceptible_hiv'
LTBI_SUSCEPTIBLE_HIV = 'ltbi_susceptible_hiv'
LTBI_POSITIVE_HIV = 'ltbi_positive_hiv'
SUSCEPTIBLE_TB_POSITIVE_HIV = 'susceptible_tb_positive_hiv'
SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV = 'susceptible_tb_susceptible_hiv'

HIV_TB_STATES = [ACTIVETB_SUSCEPTIBLE_HIV, ACTIVETB_POSITIVE_HIV,
                 LTBI_SUSCEPTIBLE_HIV, LTBI_POSITIVE_HIV,
                 SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV, SUSCEPTIBLE_TB_POSITIVE_HIV]

# transition names (from, to)
ACTIVETB_SUSCEPTIBLE_HIV_TO_ACTIVETB_POSITIVE_HIV = 'activetb_susceptible_hiv_to_activetb_positive_hiv'
ACTIVETB_SUSCEPTIBLE_HIV_TO_SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV = 'activetb_susceptible_hiv_to_susceptible_tb_susceptible_hiv'
ACTIVETB_POSITIVE_HIV_TO_SUSCEPTIBLE_TB_POSITIVE_HIV = 'activetb_positive_hiv_to_susceptible_tb_positive_hiv'
LTBI_SUSCEPTIBLE_HIV_TO_ACTIVETB_SUSCEPTIBLE_HIV = 'ltbi_susceptible_hiv_to_activetb_susceptible_hiv'
LTBI_SUSCEPTIBLE_HIV_TO_LTBI_POSITIVE_HIV = 'ltbi_susceptible_hiv_to_ltbi_positive_hiv'
LTBI_POSITIVE_HIV_TO_ACTIVETB_POSITIVE_HIV = 'ltbi_positive_hiv_to_activetb_positive_hiv'
SUSCEPTIBLE_TB_POSITIVE_HIV_TO_LTBI_POSITIVE_HIV = 'susceptible_tb_positive_hiv_to_ltbi_positive_hiv'
SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV_TO_LTBI_SUSCEPTIBLE_HIV = 'susceptible_tb_susceptible_hiv_to_ltbi_susceptible_hiv'
SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV_TO_SUSCEPTIBLE_TB_POSITIVE_HIV = 'susceptible_tb_susceptible_hiv_to_susceptible_tb_positive_hiv'

HIV_TB_TRANSITIONS = [ACTIVETB_SUSCEPTIBLE_HIV_TO_ACTIVETB_POSITIVE_HIV,
                      ACTIVETB_SUSCEPTIBLE_HIV_TO_SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV,
                      ACTIVETB_POSITIVE_HIV_TO_SUSCEPTIBLE_TB_POSITIVE_HIV,
                      LTBI_SUSCEPTIBLE_HIV_TO_ACTIVETB_SUSCEPTIBLE_HIV,
                      LTBI_SUSCEPTIBLE_HIV_TO_LTBI_POSITIVE_HIV,
                      LTBI_POSITIVE_HIV_TO_ACTIVETB_POSITIVE_HIV,
                      SUSCEPTIBLE_TB_POSITIVE_HIV_TO_LTBI_POSITIVE_HIV,
                      SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV_TO_LTBI_SUSCEPTIBLE_HIV,
                      SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV_TO_SUSCEPTIBLE_TB_POSITIVE_HIV]

CLUSTER_PROJECT = 'proj_csu'
PROJECT_NAME = 'vivarium_csu_ltbi'

LOCATIONS = ['South Africa', 'India', 'Philippines', 'Ethiopia', 'Peru']


def formatted_location(location):
    return location.replace(" ", "_").lower()


#################################
# Results columns and variables #
#################################

TOTAL_POP_COLUMN = 'total_population'
TOTAL_YLLS_COLUMN = 'years_of_life_lost'
TOTAL_YLDS_COLUMN = 'years_lived_with_disability'
RANDOM_SEED_COLUMN = 'random_seed'

TOTAL_POP_COLUMN_TEMPLATE = 'total_population_{pop_state}'
PERSON_TIME_COLUMN_TEMPLATE = 'person_time_among_{sex}_in_age_group_{age_group}_{exposure_group}'
YLDS_COLUMN_TEMPLATE = 'ylds_due_to_{cause_of_disability_state}_among_{sex}_in_age_group_{age_group}_{exposure_group}'
DEATH_COLUMN_TEMPLATE = 'death_due_to_{cause_of_death_state}_among_{sex}_in_age_group_{age_group}_{exposure_group}'
YLLS_COLUMN_TEMPLATE = 'ylls_due_to_{cause_of_death_state}_among_{sex}_in_age_group_{age_group}_{exposure_group}'
TRANSITION_EVENT_COLUMN_TEMPLATE = '{transition}_event_count_among_{sex}_in_age_group_{age_group}_{exposure_group}'
PREVALENT_CASES_COLUMN_TEMPLATE = '{disease_state}_prevalent_cases_in_{year}_among_{sex}_in_age_group_{age_group}_{exposure_group}'
POPULATION_COUNT_COLUMN_TEMPLATE = 'population_point_estimate_in_{year}_among_{sex}_in_age_group_{age_group}_{exposure_group}'
STATE_PERSON_TIME_COLUMN_TEMPLATE = '{disease_state}_person_time_among_{sex}_in_age_group_{age_group}_{exposure_group}'

SEXES = ['male', 'female']
AGE_GROUPS = [
    'early_neonatal', 'late_neonatal', 'post_neonatal'
    '1_to_4', '5_to_9',
    '10_to_14', '15_to_19',
    '20_to_24', '25_to_29',
    '30_to_34', '35_to_39',
    '40_to_44', '45_to_49',
    '50_to_54', '55_to_59',
    '60_to_64', '65_to_69',
    '70_to_74', '75_to_79',
    '80_to_84', '85_to_89',
    '90_to_94', '95_plus',
]
YEARS = ['2019', '2020', '2021', '2022', '2023', '2024']
EXPOSURE_GROUPS = [HOUSEHOLD_TUBERCULOSIS_EXPOSED, HOUSEHOLD_TUBERCULOSIS_UNEXPOSED]
POP_STATES = ['treated', 'untreated', 'living', 'dead']
CAUSE_OF_DISABILITY_STATES = [ACTIVETB_POSITIVE_HIV, ACTIVETB_SUSCEPTIBLE_HIV,
                              LTBI_POSITIVE_HIV,
                              SUSCEPTIBLE_TB_POSITIVE_HIV]
CAUSE_OF_DEATH_STATES = CAUSE_OF_DISABILITY_STATES + ['other_causes']
TRANSITIONS = HIV_TB_TRANSITIONS[:]
DISEASE_STATES = HIV_TB_STATES[:]


########################
# GBD Names for things #
########################

GBD_ACTIVE_TB_NAMES = ['drug_susceptible_tuberculosis',
                       'multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
                       'extensively_drug_resistant_tuberculosis',
                       'hiv_aids_drug_susceptible_tuberculosis',
                       'hiv_aids_multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
                       'hiv_aids_extensively_drug_resistant_tuberculosis']



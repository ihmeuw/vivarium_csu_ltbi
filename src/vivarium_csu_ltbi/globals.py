"""Global variables for ltbi project."""
import itertools


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

# Among HIV negative
SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV_TO_LTBI_SUSCEPTIBLE_HIV = f'{SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV}_to_{LTBI_SUSCEPTIBLE_HIV}'
LTBI_SUSCEPTIBLE_HIV_TO_ACTIVETB_SUSCEPTIBLE_HIV = f'{LTBI_SUSCEPTIBLE_HIV}_to_{ACTIVETB_SUSCEPTIBLE_HIV}'
ACTIVETB_SUSCEPTIBLE_HIV_TO_SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV = f'{ACTIVETB_SUSCEPTIBLE_HIV}_to_{SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV}'

# Among HIV positive
SUSCEPTIBLE_TB_POSITIVE_HIV_TO_LTBI_POSITIVE_HIV = f'{SUSCEPTIBLE_TB_POSITIVE_HIV}_to_{LTBI_POSITIVE_HIV}'
LTBI_POSITIVE_HIV_TO_ACTIVETB_POSITIVE_HIV = f'{LTBI_POSITIVE_HIV}_to_{ACTIVETB_POSITIVE_HIV}'
ACTIVETB_POSITIVE_HIV_TO_SUSCEPTIBLE_TB_POSITIVE_HIV = f'{ACTIVETB_POSITIVE_HIV}_to_{SUSCEPTIBLE_TB_POSITIVE_HIV}'

# From HIV negative to HIV positive
SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV_TO_SUSCEPTIBLE_TB_POSITIVE_HIV = f'{SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV}_to_{SUSCEPTIBLE_TB_POSITIVE_HIV}'
LTBI_SUSCEPTIBLE_HIV_TO_LTBI_POSITIVE_HIV = f'{LTBI_SUSCEPTIBLE_HIV}_to_{LTBI_POSITIVE_HIV}'
ACTIVETB_SUSCEPTIBLE_HIV_TO_ACTIVETB_POSITIVE_HIV = f'{ACTIVETB_SUSCEPTIBLE_HIV}_to_{ACTIVETB_POSITIVE_HIV}'


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

STANDARD_COLUMNS = {'total_population': TOTAL_POP_COLUMN,
                    'total_ylls': TOTAL_YLLS_COLUMN,
                    'total_ylds': TOTAL_YLDS_COLUMN,
                    'random_seed': RANDOM_SEED_COLUMN}

TOTAL_POP_COLUMN_TEMPLATE = 'total_population_{POP_STATE}'
PERSON_TIME_COLUMN_TEMPLATE = 'person_time_among_{SEX}_in_age_group_{AGE_GROUP}_{EXPOSURE_GROUP}'
YLDS_COLUMN_TEMPLATE = 'ylds_due_to_{CAUSE_OF_DISABILITY_STATE}_among_{SEX}_in_age_group_{AGE_GROUP}_{EXPOSURE_GROUP}'
DEATH_COLUMN_TEMPLATE = 'death_due_to_{CAUSE_OF_DEATH_STATE}_among_{SEX}_in_age_group_{AGE_GROUP}_{EXPOSURE_GROUP}'
YLLS_COLUMN_TEMPLATE = 'ylls_due_to_{CAUSE_OF_DEATH_STATE}_among_{SEX}_in_age_group_{AGE_GROUP}_{EXPOSURE_GROUP}'
TRANSITION_EVENT_COLUMN_TEMPLATE = '{TRANSITION}_event_count_among_{SEX}_in_age_group_{AGE_GROUP}_{EXPOSURE_GROUP}'
PREVALENT_CASES_COLUMN_TEMPLATE = '{DISEASE_STATE}_prevalent_cases_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}_{EXPOSURE_GROUP}'
POPULATION_COUNT_COLUMN_TEMPLATE = 'population_point_estimate_in_{YEAR}_among_{SEX}_in_age_group_{AGE_GROUP}_{EXPOSURE_GROUP}'
STATE_PERSON_TIME_COLUMN_TEMPLATE = '{DISEASE_STATE}_person_time_among_{SEX}_in_age_group_{AGE_GROUP}_{EXPOSURE_GROUP}'

COLUMN_TEMPLATES = {'total_population': TOTAL_POP_COLUMN_TEMPLATE,
                    'person_time': PERSON_TIME_COLUMN_TEMPLATE,
                    'ylds': YLDS_COLUMN_TEMPLATE,
                    'deaths': DEATH_COLUMN_TEMPLATE,
                    'ylls': YLLS_COLUMN_TEMPLATE,
                    'transitions': TRANSITION_EVENT_COLUMN_TEMPLATE,
                    'prevalent_cases': PREVALENT_CASES_COLUMN_TEMPLATE,
                    'population_count': POPULATION_COUNT_COLUMN_TEMPLATE,
                    'state_person_time': STATE_PERSON_TIME_COLUMN_TEMPLATE}

SEXES = ['male', 'female']
AGE_GROUPS = [
    'early_neonatal', 'late_neonatal', 'post_neonatal',
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
TREATMENT_GROUPS = ['untreated', '6H_adherent', '6H_nonadherent', '3HP_adherent', '3HP_nonadherent']
POP_STATES = ['treated', 'untreated', 'living', 'dead']
CAUSE_OF_DISABILITY_STATES = [ACTIVETB_POSITIVE_HIV, ACTIVETB_SUSCEPTIBLE_HIV,
                              LTBI_POSITIVE_HIV,
                              SUSCEPTIBLE_TB_POSITIVE_HIV]
CAUSE_OF_DEATH_STATES = CAUSE_OF_DISABILITY_STATES + ['other_causes']
TRANSITIONS = HIV_TB_TRANSITIONS[:]
DISEASE_STATES = HIV_TB_STATES[:]

TEMPLATE_FIELD_MAP = {'SEX': SEXES,
                      'AGE_GROUP': AGE_GROUPS,
                      'YEAR': YEARS,
                      'EXPOSURE_GROUP': EXPOSURE_GROUPS,
                      'POP_STATE': POP_STATES,
                      'CAUSE_OF_DISABILITY_STATE': CAUSE_OF_DISABILITY_STATES,
                      'CAUSE_OF_DEATH_STATE': CAUSE_OF_DEATH_STATES,
                      'TRANSITION': TRANSITIONS,
                      'DISEASE_STATE': DISEASE_STATES}


def RESULT_COLUMNS(kind='all'):
    if kind not in COLUMN_TEMPLATES and kind != 'all':
        raise ValueError(f'Unknown result column type {kind}')
    columns = []
    if kind == 'all':
        for k in COLUMN_TEMPLATES:
            columns += RESULT_COLUMNS(k)
        columns = list(STANDARD_COLUMNS.values()) + columns
    else:
        template = COLUMN_TEMPLATES[kind]
        filtered_field_map = {field: values for field, values in TEMPLATE_FIELD_MAP.items() if field in template}
        fields, value_groups = filtered_field_map.keys(), itertools.product(*filtered_field_map.values())
        for value_group in value_groups:
            columns.append(template.format(**{field: value for field, value in zip(fields, value_group)}))
    return columns



########################
# GBD Names for things #
########################

GBD_ACTIVE_TB_NAMES = ['drug_susceptible_tuberculosis',
                       'multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
                       'extensively_drug_resistant_tuberculosis',
                       'hiv_aids_drug_susceptible_tuberculosis',
                       'hiv_aids_multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
                       'hiv_aids_extensively_drug_resistant_tuberculosis']



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

TEMPLATE_CAUSES = [ACTIVETB_POSITIVE_HIV, ACTIVETB_SUSCEPTIBLE_HIV,
                   LTBI_POSITIVE_HIV, LTBI_SUSCEPTIBLE_HIV,
                   SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV, SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV,
                   'other_causes']


GBD_ACTIVE_TB_NAMES = ['drug_susceptible_tuberculosis',
                       'multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
                       'extensively_drug_resistant_tuberculosis',
                       'hiv_aids_drug_susceptible_tuberculosis',
                       'hiv_aids_multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
                       'hiv_aids_extensively_drug_resistant_tuberculosis']


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




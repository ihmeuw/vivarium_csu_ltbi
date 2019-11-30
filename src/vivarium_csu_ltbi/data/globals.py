

COUNTRIES = ['South Africa', 'India', 'Philippines', 'Ethiopia', 'Peru']


def formatted_country(country):
    return country.replace(" ", "_").lower()


ACTIVE_TB_NAMES = ['drug_susceptible_tuberculosis',
                   'multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
                   'extensively_drug_resistant_tuberculosis',
                   'hiv_aids_drug_susceptible_tuberculosis',
                   'hiv_aids_multidrug_resistant_tuberculosis_without_extensive_drug_resistance',
                   'hiv_aids_extensively_drug_resistant_tuberculosis']

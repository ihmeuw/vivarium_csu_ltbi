*****************************
Artifact Reproduction Steps
*****************************
Much of the data that goes into the LTBI artifact is a product of extra-GBD
analysis and modeling. It must be prepared and up-to-date before the artifact
building process can proceed. The following details the requisite data.

Household Tuberculosis Exposure
+++++++++++++++++++++++++++++++

Household TB exposure is modeled by the procedures found in
data/household_tb_model.py. It takes a while, so the CLI contains an entrypoint
for collecting the necessary input data (`get_household_tb_input_data`) and
for submitting modeling jobs in parallel, `get_household_tb_parallel`.

Latent Tuberculosis Incidence
+++++++++++++++++++++++++++++

Latent Tuberculosis is similar to Household TB exposure in that it requires
extra modeling and takes a while. The modeling procedure is defined in
data/ltbi_incidence_model.py. We have similar entrypoints,
`get_ltbi_incidence_input_data` and `get_ltbi_incidence_input_data`.

Adjusting Coverage Shift Data
+++++++++++++++++++++++++++++

The drug coverages are defined in terms of baseline coverages and deltas to the
baseline, found in data/baseline_coverages.csv and
data/intervention_coverage_shift_raw.csv. The drug 3HP is not approved for use
use in children under the age of two, so a reproducible procedure defined in
data/adjust_coverage_shift_data.py introduces age and defines the coverage for
children under two as 6H, saving it in data/intervention_coverage_shift.csv

Treatment Adherence Draws
+++++++++++++++++++++++++

The treatment adherence and efficacy data was generated using an ipython notebook
that has been exported to a script and placed here, called "ltbi_tx_efficacy_and_adherence_model.py".
It saves data in the same directory, called "treatment_adherence_draws.csv." This CSV
is used directly in the artifact building procedure.

Introduction
============

In our LTBI simulation, we developed new data and a Vivarium model for a risk factor that is not in GBD, but is central to current TB control efforts: household exposure to Active Tuberculosis (hh_exposure_to_actb).  This required systematic review and meta-analysis of the relative risk of developing LTBI among individuals with this risk exposure (relative to individuals without it), as well as a cool, new approach to calculating the age-/sex-/location-specific fraction of individuals living with this risk exposure.

In our Vivarium modeling so far, we have used our standard model of risk exposure to implement this new risk factor.  This approach endows each simulant with a risk factor propensity, which is held constant for the duration of the sim, and therefore has an unrealistically low “turnover rate”, meaning that, in the sim, individuals live with this risk exposure for longer than is realistic.

This scoping document defines and justifies the model changes that will be necessary to increase the turnover rate for the hh_exposure_to_actb risk factor.  It also proposes a slightly more complicated model to accomplish this, where each individual changes their hh_exposure_to_actb propensity at randomly selected times.


Methods
=======

Definition of turnover time
---------------------------

The _turnover rate_ is jargon we have adapted or invented for this scoping document, and it refers to the amount of time an individual spends in the higher-risk category of the hh_exposure_to_actb risk factor exposure.  To be precise, we define it analogously to a remission rate:

Turnover rate = (# individuals who transition from cat1 to cat2) / (person-years of individuals observed in cat1)

Since it takes about six months of treatment to clear a drug-susceptible Active TB infection, we expect the turnover rate to be a bit less than (1 transition) / (0.5 person-years) = 2.0 --- the precise amount less probably depends on how long it typically takes the health system to diagnose and initiate treatment for a person living with Active TB.


Why turnover time is probably lower than current sim affords
----------------------------------------------------------

Although there is probably some transition from cat1 to cat2 when simulants change age-groups in our current sim, the numerator in the turnover rate is nearly zero, as simulants mostly remain in the exposure category that they were initialized into (since their propensity remains constant).  This is something we could measure empirically with a custom observer, but so far we have not done so (as far as I know).


Why a lower turnover time might substantially change simulation results
-----------------------------------------------------------------------

Since a central question in our LTBI sim is about how many children will develop Active TB under different treatment approaches for children with household exposure to people living with active tuberculosis, the number of simulants who spend time in cat1 of this risk will be directly related to the number of cases of active TB that are prevented when treatment is scaled up.

A realistic turnover rate of 2.0 corresponds to around 6 months of exposure per person, while our current sim produces around 5 years of exposure per person. Thus we might expect this change to lead to ten times more simulants spending some time in cat1, and therefor ten times more children being treated and not progressing to Active TB.


A simple way to parameterize and implement turnover time
--------------------------------------------------------

Clearly, there is a lot of work that could eventually go into improvements to the change in risk factor exposure over time.  For this model, a simple way to parameterize and implement a variable turnover rate is to add a model parameter for this turnover rate (expressed in transitions per person-year) and to use it every timestep to determine if each individual gets a new propensity (sampled uniformly from the interval [0,1]) from which to map their hh_exposure_to_actb category.  To be precise, with probability p = exp(-rate*(size of time step in years)) an individual’s propensity would remain unchanged, while with probability 1-p the individual’s propensity would be replaced with a value sampled uniformly at random from [0,1].

This (I believe) would produce a turnover rate of rate transitions per person-year, while maintaining a uniform distribution of the propensities at any instant in time, and therefore remaining calibrated with the exposure fraction we calculated from the household structure data and the GBD prevalence rates.

Results
=======

Our current approach to modeling the hh_exposure_to_actb risk factor includes some change over time; as individuals age, their propensity is compared to an age-group-specific threshold, and therefore some individuals will transition between exposed and unexposed during the course of the simulation. However, the average turnover time in this approach is years, not months. (Table X)

In the new approach proposed in this scoping document, the propensity for hh_exposure_to_actb itself will change at random times, selected with a rate set by a turnover parameter, which provides explicit control of the average turnover time. (Table X)

In both approaches, the calibration of the age-/sex-/location-/time-specific exposure level relies on the propensities being uniformly distributed between zero and one. In the current approach, this is accomplished by drawing the propensities from a uniform distribution once, when initializing a simulants.  In the new proposed approach, the propensities are resampled at randomly chosen times during the simulation, but each random time is independent of the current propensity value, so the marginal distribution of propensities remains uniform. (Figure Y)


Discussion
==========

Process for scoping documents
-----------------------------

I hope that this helps us generate a template for researchers to use when scoping new feature requests to the engineering team. We should keep in mind what is generalizable, and what is missing, and refactor that into the vivarium_research repository at some point in the near future.


Additional limitations
----------------------

This model of risk factor exposure ignores all additional correlations and “common cause” relationships, such as occupational exposure to active tuberculosis, the posited association between lower socioeconomic status and higher incidence of tuberculosis, etc, that could make our more “more realistic”.  Assessing the evidence base and data availability, as well as the potential impact of these additional factors is left as important future work.


Future improvements along these same lines
------------------------------------------

Many of our risk factor exposures are limited by the approach we have taken that holds propensity constant for the duration of the sim.  In this case, the “autocorrelation” structure was simple enough to be handled by a single parameter “turnover rate” model, and this approach might be relevant to other dichotomous risks.  For continuous risks, it might be possible to design other propensity change-over-time approaches that vary more gradually to match relevant parameters measured from longitudinal data, for example about fluctuations in HAZ or WHZ among children or BMI or SBP among adults.


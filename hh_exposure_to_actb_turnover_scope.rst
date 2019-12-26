Introduction
============

In our LTBI simulation, we developed new data and a Vivarium model for a risk factor that is not in GBD, but is central to current TB control efforts: household exposure to Active Tuberculosis (hh_exposure_to_actb).  This required systematic review and meta-analysis of the relative risk of developing LTBI among individuals with this risk exposure (relative to individuals without it), as well as a cool, new approach to calculating the age-/sex-/location-specific fraction of individuals living with this risk exposure.

In our Vivarium modeling so far, we have used our standard model of risk exposure to implement this new risk factor.  This approach endows each simulant with a risk factor propensity, which is held constant for the duration of the sim, and therefore has an unrealistically low “turnover rate”, meaning that, in the sim, individuals live with this risk exposure for longer than is realistic.

This scoping document defines and justifies the model changes that will be necessary to increase the turnover rate for the hh_exposure_to_actb risk factor.  It also proposes a slightly more complicated model to accomplish this, where each individual changes their hh_exposure_to_actb propensity at randomly selected times.


Methods
=======

Definition of turnover time
---------------------------

Why turnover time is probably lower than current sim affords
----------------------------------------------------------

Why a lower turnover time might substantially change simulation results
-----------------------------------------------------------------------

A simple way to parameterize and implement turnover time
--------------------------------------------------------

Results
=======

Our current approach to modeling the hh_exposure_to_actb risk factor includes some change over time; as individuals age, their propensity is compared to an age-group-specific threshold, and therefore some individuals will transition between exposed and unexposed during the course of the simulation. However, the average turnover time in this approach is years, not months. (Table X)

In the new approach proposed in this scoping document, the propensity for hh_exposure_to_actb itself will change at random times, selected with a rate set by a turnover parameter, which provides explicit control of the average turnover time. (Table X)

In both approaches, the calibration of the age-/sex-/location-/time-specific exposure level relies on the propensities being uniformly distributed between zero and one. In the current approach, this is accomplished by drawing the propensities from a uniform distribution once, when initializing a simulants.  In the new proposed approach, the propensities are resampled at randomly chosen times during the simulation, but each random time is independent of the current propensity value, so the marginal distribution of propensities remains uniform. (Figure Y)


Discussion
==========

Process for scoping documents
-----------------------------

Additional limitations
----------------------

Future improvements along these same lines
------------------------------------------


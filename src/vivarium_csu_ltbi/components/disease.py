#from vivarium.framework.state_machine import Trigger, Transition
from vivarium_public_health.disease import DiseaseState, DiseaseModel, SusceptibleState
from .names import *


def wrap_data_getter(id):
    return {
        id: lambda _, builder: builder.data.load(
            f'sequela.{id}.prevalence'
        )
    }


def get_disease_state(id):
    ds = DiseaseState(id, cause_type='sequela', get_data_functions=wrap_data_getter(id))
    ds.allow_self_transitions()
    return ds

#
# def add_transitions(from_state, to_states):
#     for state in to_states:
#         from_state.add_transition(state, source_data_type='rate', get_data_functions=lambda _: None)
#     return from_state


def susceptible_tb_susceptible_hiv():
    # the non-disease state
    susceptible = SusceptibleState(SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV)
    susceptible.allow_self_transitions()

    ds_ltbi_sus_hiv = get_disease_state(LTBI_SUSCEPTIBLE_HIV)
    ds_acttb_sus_hiv = get_disease_state(ACTIVETB_SUSCEPTIBLE_HIV)
    ds_recltbi_sus_hiv = get_disease_state(RECOVERED_LTBI_SUSCEPTIBLE_HIV)
    ds_recltbi_pos_hiv = get_disease_state(RECOVERED_LTBI_POSITIVE_HIV)
    ds_sus_tb_pos_hiv = get_disease_state(SUSCEPTIBLE_TB_POSITIVE_HIV)
    ds_ltbi_pos_hiv = get_disease_state(LTBI_POSITIVE_HIV)
    ds_acttb_pos_hiv = get_disease_state(ACTIVETB_POSITIVE_HIV)


    return DiseaseModel(LATENT_TUBERCULOSIS_INFECTION,
                        states=[susceptible,
                                ds_ltbi_sus_hiv,
                                ds_acttb_sus_hiv,
                                ds_recltbi_sus_hiv,
                                ds_recltbi_pos_hiv,
                                ds_sus_tb_pos_hiv,
                                ds_ltbi_pos_hiv,
                                ds_acttb_pos_hiv
                                ])

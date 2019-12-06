from vivarium_public_health.disease import DiseaseState, DiseaseModel, SusceptibleState, RateTransition

import vivarium_csu_ltbi.globals as ltbi_globals


def wrap_data_getter(id):
    return {
        id: lambda _, builder: builder.data.load(
            f'sequela.{id}.prevalence'
        )
    }


def get_disease_state(id):
    ds = BetterDiseaseState(id, cause_type='sequela', get_data_functions=wrap_data_getter(id))
    ds.allow_self_transitions()
    return ds


class BetterDiseaseState(DiseaseState):
    def add_transition(self, output, source_data_type=None, get_data_functions=None, **kwargs):
        if get_data_functions == None:
            get_data_functions = {'transition_rate': lambda cause, builder: builder.data.load(
                f'sequela.{self.cause}_to_{cause}.transition_rate')}
        t = BetterRateTransition(self, output, get_data_functions, **kwargs)
        self.transition_set.append(t)
        return t


class BetterSusceptibleState(SusceptibleState):
    def add_transition(self, output, source_data_type=None, get_data_functions=None, **kwargs):
        if get_data_functions == None:
            get_data_functions = {'transition_rate': lambda cause, builder: builder.data.load(
                f'sequela.{self.cause}_to_{cause}.transition_rate')}
        t = BetterRateTransition(self, output, get_data_functions, **kwargs)
        self.transition_set.append(t)
        return t


class BetterRateTransition(RateTransition):
    def load_transition_rate_data(self, builder):
        if 'transition_rate' in self._get_data_functions:
            rate_data = self._get_data_functions['transition_rate'](self.output_state.cause, builder)
            pipeline_name = f'{self.input_state.state_id}_to_{self.output_state.state_id}.transition_rate'
        else:
            raise ValueError("No valid data functions supplied.")
        return rate_data, pipeline_name


def TuberculosisAndHIV():
    # the non-disease state
    susceptible = BetterSusceptibleState(ltbi_globals.SUSCEPTIBLE_TB_SUSCEPTIBLE_HIV)
    susceptible.allow_self_transitions()

    # states
    ltbi_susceptible_hiv = get_disease_state(ltbi_globals.LTBI_SUSCEPTIBLE_HIV)
    activetb_susceptible_hiv = get_disease_state(ltbi_globals.ACTIVETB_SUSCEPTIBLE_HIV)
    susceptible_tb_positive_hiv = get_disease_state(ltbi_globals.SUSCEPTIBLE_TB_POSITIVE_HIV)
    ltbi_positive_hiv = get_disease_state(ltbi_globals.LTBI_POSITIVE_HIV)
    activetb_positive_hiv = get_disease_state(ltbi_globals.ACTIVETB_POSITIVE_HIV)

    # transitions
    susceptible.add_transition(ltbi_susceptible_hiv)
    susceptible_tb_positive_hiv.add_transition(ltbi_positive_hiv)

    susceptible.add_transition(susceptible_tb_positive_hiv)

    ltbi_susceptible_hiv.add_transition(activetb_susceptible_hiv)
    ltbi_susceptible_hiv.add_transition(ltbi_positive_hiv)

    ltbi_positive_hiv.add_transition(activetb_positive_hiv)

    activetb_susceptible_hiv.add_transition(susceptible)
    activetb_susceptible_hiv.add_transition(activetb_positive_hiv)

    activetb_positive_hiv.add_transition(susceptible_tb_positive_hiv)

    return DiseaseModel(ltbi_globals.TUBERCULOSIS_AND_HIV,
                        states=[susceptible,
                                ltbi_susceptible_hiv,
                                activetb_susceptible_hiv,
                                susceptible_tb_positive_hiv,
                                ltbi_positive_hiv,
                                activetb_positive_hiv
                                ])

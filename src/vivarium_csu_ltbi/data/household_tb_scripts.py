import pandas as pd
import numpy as np


def estimate_household_tb(country_name: str, year_start=2017):
    """for certain country in year 2017,
    sweep over draw
    """
    # outputs = pd.DataFrame()
    # df_hh = load_hh_data(country_name)
    # hh_ids = df_hh.hh_id.unique()
    # prev_actb = load_and_transform(country_name)

    for draw in range(1000):
        # bootstrap HH data by resampling hh_id with replacement
        sample_hhids = np.random.choice(hh_ids, size=len(hh_ids), replace=True)
        df_hh_sample = pd.DataFrame()
        for i in sample_hhids:
            df_hh_sample = df_hh_sample.append(df_hh[df_hh.hh_id == i])

        data = interpolation(prev_actb, df_hh_sample, year_start, draw)
        res = age_sex_specific_actb_prop(data)
        res['location'] = country_name
        res['year_start'] = year_start
        res['year_end'] = year_start + 1
        res['draw'] = draw
        outputs = pd.concat([outputs, res], ignore_index=True)

    outputs = outputs.set_index(index_cols)
    return outputs


def collect_household_tb():
    pass



if __name__=='__main__':
    pass

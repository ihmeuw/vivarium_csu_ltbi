import numpy as np


def aggregate_over_treatment_group(data):
    groupby_cols = [c for c in data.columns if c not in ['treatment_group', 'value']]
    data = (data
            .groupby(groupby_cols)
            .value.sum()
            .reset_index())
    data['treatment_group'] = 'all'
    return data


def pivot_and_summarize(data, index_columns, prefix=''):
    data = (data
            .set_index(index_columns + ['draw'])
            .unstack())
    data.columns = data.columns.droplevel()
    data.columns.name = None
    mean = data.mean(axis=1)
    # CI = 95%
    ub = np.percentile(data, 97.5, axis=1)
    lb = np.percentile(data, 2.5, axis=1)
    data[prefix + 'mean'] = mean
    data[prefix + 'ub'] = ub
    data[prefix + 'lb'] = lb
    data = data[[c for c in data.columns if isinstance(c, str)]]
    return data

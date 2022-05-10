import ast

import pandas as pd
from entropylab.api.in_process_param_store import InProcessParamStore
from entropylab.api.param_store import ParamStore
import itertools
from dash.exceptions import PreventUpdate

params = InProcessParamStore()
params.q1 = 1000
params.q2 = 1234
params.q3 = 1234
params.q4 = 1234
params.add_tag('qubits', 'q1')
params.add_tag('qubits', 'q2')
params.add_tag('qubits', 'q3')
params.add_tag('qubits', 'q4')

params.commit('setup')
params.save_temp()

def paramStore_to_df(ps: ParamStore):
    ps_dict = ps.to_dict()
    # tag_list =list(itertools.chain(*[ps.list_tags_for_key(key) for key in ps.keys()]))
    tag_list=[]
    for key in ps.keys():
        if not ps.list_tags_for_key(key):
            tag_list.append('')
        else:
            tag_list.append(ps.list_tags_for_key(key))
    tag_list=["['spam','eggs']"] * len(ps)
    params_df = pd.DataFrame({"key": ps_dict.keys(), "value": ps_dict.values(), "tag": tag_list})
    # cols = [
    #     dict(name="key", id="key", type="text"),
    #     dict(name="value", id="value", type="any"),
    #     dict(name="tag", id="tag", type="text"),
    # ]
    return params_df


def paramStore_commits_df(ps: ParamStore):
    commit_list = ps.list_commits()
    ids = []
    labels = []
    times = []
    for commit in commit_list:
        ids.append(commit.id)
        labels.append(commit.label)
        times.append(pd.Timestamp(commit.ns, unit='ns').strftime('%Y-%m-%d %X'))
    return pd.DataFrame({"commit_id": ids, "commit_label": labels, "commit_time": times})


def data_diff(params,data, data_prev):
    if len(data) > len(data_prev):
        return data_prev[-1]
    if len(data) < len(data_prev):
        del_key = set([x['key'] for x in data_prev]).difference(set([x['key'] for x in data]))
        del params[del_key.pop()]
    for idx, row in enumerate(data):
        if (len(data_prev) == len(data)) and not data_prev[idx] == data[idx]:
            if not data_prev[idx]['key']==data[idx]['key']:
                params.rename_key(data_prev[idx]['key'],data[idx]['key'])
            params[data[idx]['key']] = data[idx]['value']

            print(data[idx]['tag'])
            return

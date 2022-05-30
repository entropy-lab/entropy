import pandas as pd

from entropylab.pipeline.api.param_store import ParamStore


def param_store_to_df(ps: ParamStore):
    ps_dict = ps.to_dict()
    tag_list = []
    for key in ps.keys():
        tags = ps.list_tags_for_key(key)
        tag_list.append(",".join(tags))
    params_df = pd.DataFrame(
        {"key": ps_dict.keys(), "value": ps_dict.values(), "tag": tag_list}
    )
    return params_df


def param_store_to_commits_df(ps: ParamStore):
    commit_list = ps.list_commits()
    ids = []
    labels = []
    times = []
    for commit in commit_list:
        ids.append(commit.id)
        labels.append(commit.label)
        times.append(pd.Timestamp(commit.timestamp, unit="ns").strftime("%Y-%m-%d %X"))
    return pd.DataFrame(
        {"commit_id": ids, "commit_label": labels, "commit_time": times}
    )


def data_diff(params: ParamStore, data, data_prev):
    if len(data) > len(data_prev):
        return data_prev[-1]
    if len(data) < len(data_prev):
        del_key = set([x["key"] for x in data_prev]).difference(
            set([x["key"] for x in data])
        )
        del params[del_key.pop()]
    for idx, _row in enumerate(data):
        if (len(data_prev) == len(data)) and not data_prev[idx] == data[idx]:
            if not data_prev[idx]["key"] == data[idx]["key"]:
                params.rename_key(data_prev[idx]["key"], data[idx]["key"])
            params[data[idx]["key"]] = data[idx]["value"]
            if not data_prev[idx]["tag"] == data[idx]["tag"]:
                params.remove_tag(data_prev[idx]["tag"], data[idx]["key"])
                params.add_tag(data[idx]["tag"], data[idx]["key"])

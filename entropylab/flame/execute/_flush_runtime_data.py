import json
from sqlalchemy import text as sql_text

# typing
from typing import Tuple, Optional, NoReturn, Dict, Any, Iterable, Union, List
from h5py import File, Group
from sqlalchemy.engine import Connection


def _flush_into_hdf5(
    grp: Group,
    node_info_outputs: Dict[str, Any],
    output_name: str,
    results: Any,
    results_time: Union[str, List[str]],
) -> NoReturn:
    dset = grp.create_dataset(f"{output_name}_time", data=results_time)
    try:
        dset = grp.create_dataset(output_name, data=results)
    except TypeError:
        r = []
        for elem in results:
            r.append(json.dumps(elem))
        dset = grp.create_dataset(output_name, data=r)
    dset.attrs["description"] = node_info_outputs["description"][output_name]
    dset.attrs["units"] = node_info_outputs["units"][output_name]


def _get_node_output(
    db: Connection,
    node_name: str,
    output_name: str,
    node_info_outputs_retentions: Dict[str, int],
) -> Optional[Tuple]:
    eui = f"#{node_name}/{output_name}"

    if node_info_outputs_retentions[output_name] != 2:
        return None
    results = db.execute(sql_text(f'SELECT value FROM "{eui}"')).scalars().all()
    results_time = (
        db.execute(
            sql_text(
                f"""
SELECT to_char
(time::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
FROM "{eui}"
"""
            )
        )
        .scalars()
        .all()
    )
    return results, results_time


def update_node(
    f: File,
    db: Connection,
    node_name: str,
    node: object,
    node_info: Dict[str, Any],
) -> NoReturn:
    grp = f.create_group(f"{node_name}")
    grp.attrs["type"] = node_info["name"]
    grp.attrs["description"] = node_info["description"]
    grp.attrs["bin"] = node_info["bin"]

    for output_name in node._outputs._outputs:
        node_info_outputs = node_info["outputs"][0]
        result = _get_node_output(
            db, node_name, output_name, node_info_outputs["retention"]
        )
        if result is not None:
            _flush_into_hdf5(grp, node_info_outputs, output_name, result[0], result[1])

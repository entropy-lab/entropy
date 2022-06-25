import json
from sqlalchemy import text as sql_text

# typing
from collections import Mapping
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


def _simplify_output_json_structure(structure):
    if not isinstance(structure, Iterable) or isinstance(structure, str):
        return None
    if isinstance(structure, Mapping):
        return {
            key: _simplify_output_json_structure(value)
            for key, value in structure.items()
        }
    # iterable items
    structure = [_simplify_output_json_structure(item) for item in structure]
    if all(item is None for item in structure):
        return None
    return structure


def _flush_structure_into_json(
    json_structure: Dict,
    node_name: str,
    output_name: str,
    results: Any,
    results_time: Union[str, List[str]],
):
    json_structure.update(
        {node_name: {output_name: results, f"{output_name}_time": results_time}}
    )


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
) -> Dict[str, Any]:
    node_output_data_structure = {}
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
            _flush_structure_into_json(
                node_output_data_structure, node_name, output_name, result[0], result[1]
            )
    print(node_output_data_structure)
    simplified = _simplify_output_json_structure(node_output_data_structure)
    print(simplified)
    return simplified

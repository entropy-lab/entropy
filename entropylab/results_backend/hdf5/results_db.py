from datetime import datetime
from typing import Optional, Any

import h5py

from entropylab import RawResultData

HDF_FILENAME = "./entropy.hdf5"


def experiment_from(dset):
    return dset.attrs['experiment']


def stage_from(dset):
    return dset.attrs['stage']


def label_from(dset):
    return dset.attrs['label']


def story_from(dset):
    return dset.attrs['story']


def data_from(dset):
    if dset.dtype.metadata is not None and dset.dtype.metadata.get('vlen') == str:
        return dset.asstr()[()]
    else:
        return dset[()]


def build_raw_result_data(dset: h5py.Dataset) -> RawResultData:
    return RawResultData(
        stage=stage_from(dset),
        label=label_from(dset),
        data=data_from(dset),
        story=story_from(dset))


def build_key(dset: h5py.Dataset) -> (int, int, str):
    return experiment_from(dset), stage_from(dset), label_from(dset)


def get_children_or_by_name(group: h5py.Group, name: Optional[str] = None):
    """
    Returns all or one child from an h5py.Group

    Parameters
    ----------
    group group to get child or children from. Can be h5py.File itself.
    name name of child to get. If None, indicates all children should be retrieved.

    Returns
    -------
    A list of group children (either h5py.Group or h5py.Datasets)
    """
    if name is None:
        return list(group.values())
    else:
        if name in group:
            return [group[name]]
        else:
            return []


# noinspection PyMethodMayBeStatic
class ResultsDB:

    def __init__(self):
        pass

    def save_result(self, experiment_id: int, result: RawResultData) -> str:
        # TODO: limit characters in label to fit hdf5???
        with h5py.File(HDF_FILENAME, 'a') as file:
            path = f"/{experiment_id}/{result.stage}"
            group = file.require_group(path)
            dset = group.create_dataset(
                name=result.label,
                data=result.data)
            dset.attrs.create('experiment', experiment_id)
            dset.attrs.create('stage', result.stage)
            dset.attrs.create('label', result.label or "")
            dset.attrs.create('story', result.story or "")
            dset.attrs.create('time', datetime.now().astimezone().isoformat())
            return dset.name

    def read_result(self, experiment_id: int, stage: int, label: str) -> RawResultData:
        with h5py.File(HDF_FILENAME, 'r') as file:
            path = f"/{experiment_id}/{stage}/{label}"
            dset = file.get(path)
            return build_raw_result_data(dset)

    def get_results(
            self,
            experiment_id: Optional[int] = None,
            stage: Optional[int] = None,
            label: Optional[str] = None,
    ) -> dict[(str, str), Any]:
        """
        Returns a dictionary of RawDataResults from HDF5 in the form of a dictionary where the key is a tuple of
        experiment, stage and label and the value is the raw data.
        """
        result = {}
        with h5py.File(HDF_FILENAME, 'r') as file:
            experiments = get_children_or_by_name(file, str(experiment_id))
            for experiment in experiments:
                stages = get_children_or_by_name(experiment, str(stage))
                for stage in stages:
                    dsets = get_children_or_by_name(stage, label)
                    for dset in dsets:
                        result[build_key(dset)] = build_raw_result_data(dset)
        return result

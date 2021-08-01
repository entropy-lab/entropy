from datetime import datetime
from typing import Optional, Any, Iterable

import h5py

from entropylab import RawResultData
from entropylab.api.data_reader import ResultRecord

HDF_FILENAME = "./entropy.hdf5"


def experiment_from(dset: h5py.Dataset) -> int:
    return dset.attrs['experiment']


def id_from(dset: h5py.Dataset) -> str:
    return dset.name


def stage_from(dset: h5py.Dataset) -> int:
    return dset.attrs['stage']


def label_from(dset: h5py.Dataset) -> str:
    return dset.attrs['label']


def story_from(dset: h5py.Dataset) -> str:
    return dset.attrs['story']


def data_from(dset: h5py.Dataset) -> Any:
    if dset.dtype.metadata is not None and dset.dtype.metadata.get('vlen') == str:
        return dset.asstr()[()]
    else:
        return dset[()]


def time_from(dset: h5py.Dataset) -> datetime:
    return datetime.fromisoformat(dset.attrs['time'])


def build_raw_result_data(dset: h5py.Dataset) -> RawResultData:
    return RawResultData(
        stage=stage_from(dset),
        label=label_from(dset),
        data=data_from(dset),
        story=story_from(dset))


def build_result_record(dset: h5py.Dataset) -> ResultRecord:
    return ResultRecord(
        experiment_id=experiment_from(dset),
        # TODO: How to generate a numeric id? Or refactor id to str?
        id=0,  # id_from(dset),
        label=label_from(dset),
        story=story_from(dset),
        stage=stage_from(dset),
        data=data_from(dset),
        time=time_from(dset))


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
        if str(name) in group:
            return [group[str(name)]]
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
            # TODO: What if there is no Label? Is this possible? Enforce it!
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
    ) -> Iterable[ResultRecord]:
        """
        Returns a list ResultRecords from HDF5.
        """
        result = []
        try:
            with h5py.File(HDF_FILENAME, 'r') as file:
                if file is None:
                    return
                experiments = get_children_or_by_name(file, experiment_id)
                for experiment in experiments:
                    stages = get_children_or_by_name(experiment, stage)
                    for stage in stages:
                        dsets = get_children_or_by_name(stage, label)
                        for dset in dsets:
                            result.append(build_result_record(dset))
        except FileNotFoundError:
            return result
        return result

    def get_last_result_of_experiment(
        self, experiment_id: int
    ) -> Optional[ResultRecord]:
        results = self.get_results(experiment_id, None, None)
        if results:
            results.sort(key=lambda x: x.time, reverse=True)
            return results[0]
        else:
            return None

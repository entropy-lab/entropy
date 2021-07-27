from typing import Optional, Any

import h5py

from entropylab import RawResultData

# noinspection PyMethodMayBeStatic
HDF_FILENAME = "./entropy.hdf5"


class ResultsDB:

    def __init__(self):
        pass

    def __get_filename(self, experiment_id: int) -> str:
        return f"experiment_{experiment_id}.hdf5"

    def __get_group_name(self, stage: int) -> str:
        return f"stage_{stage}"

    def __get_dset_name(self, label: str) -> str:
        return f"label_{label}"

    def write_result(self, experiment_id: int, result: RawResultData):
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

    def read_result(self, experiment_id: int, stage: int, label: str):
        with h5py.File(HDF_FILENAME, 'r') as file:
            path = f"/{experiment_id}/{stage}/{label}"
            dset = file.get(path)
            # TODO: strings come back a byte arrays...?
            return dset[()]

    def get_results(
            self,
            experiment_id: Optional[int] = None,
            stage: Optional[int] = None,
            label: Optional[str] = None,
    ) -> dict[(str, str), Any]:
        """
        Returns a set of results data from HDF5, in the form of a dictionary where the key is the stage and the value
        is the raw data.
        """
        filename = self.__get_filename(experiment_id)
        with h5py.File(filename, 'r') as file:
            data_dict = {}
            file.visititems(self.add_item_to_dict(data_dict))
            return data_dict

    def get_labels(self, group: h5py.Group, label: Optional[str] = None):
        if label is None:
            return list(group.values())
        else:
            if label in group:
                return [group[label]]
            else:
                return []

    def add_item_to_dict(self, dsets: []):
        def visitor(name, item):
            if isinstance(item, h5py.Dataset):
                dsets[(item.attrs['stage'], item.attrs['label'])] = item[()]
            return None

        return visitor

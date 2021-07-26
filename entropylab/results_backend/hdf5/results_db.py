from typing import Optional, Any

import h5py

from entropylab import RawResultData


# noinspection PyMethodMayBeStatic
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
        filename = self.__get_filename(experiment_id)
        group_name = self.__get_group_name(result.stage)
        dset_name = self.__get_dset_name(result.label)
        # TODO: limit characters in label to fit hdf5???
        with h5py.File(filename, 'a') as file:
            # file (experiment)
            file.attrs.create('experiment', experiment_id)
            # group (stage)
            group = file.require_group(group_name)
            group.attrs.create('experiment', experiment_id)
            group.attrs.create('stage', result.stage)
            # dataset (label)
            dset = group.create_dataset(
                name=dset_name,
                data=result.data)
            dset.attrs.create('experiment', experiment_id)
            dset.attrs.create('stage', result.stage)
            dset.attrs.create('label', result.label or "")
            dset.attrs.create('story', result.story or "")

    def read_result(self, experiment_id: int, stage: int, label: str):
        filename = self.__get_filename(experiment_id)
        group_name = self.__get_group_name(stage)
        dset_name = self.__get_dset_name(label)
        # TODO: What if file does not exist?
        with h5py.File(filename, 'r') as file:
            # TODO: What if dset does not exist?
            group = file.get(group_name)
            # TODO: What if group does not exist?
            dset = group.get(dset_name)
            # TODO: strings come back a byte arrays...?
            return dset[()]

    def get_results(
            self,
            # TODO: How to get results across experiments?
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

    def add_item_to_dict(self, dsets: []):
        def visitor(name, item):
            if isinstance(item, h5py.Dataset):
                dsets[(item.attrs['stage'], item.attrs['label'])] = item[()]
            return None

        return visitor

from typing import Optional, Iterable, Any

import h5py

from entropylab import RawResultData


# noinspection PyMethodMayBeStatic
class ResultsDB:

    def __init__(self):
        pass

    def __get_filename(self, experiment_id: int) -> str:
        return f"experiment_{experiment_id}.hdf5"

    def __get_dset_name(self, stage: int) -> str:
        return f"stage_{stage}"

    def write_result(self, experiment_id: int, result: RawResultData):
        filename = self.__get_filename(experiment_id)
        # dataset per stage X label
        # TODO: limit characters in label to fit hdf5???
        dset_name = self.__get_dset_name(result.stage)
        with h5py.File(filename, 'a') as file:
            dset = file.create_dataset(
                name=dset_name,
                data=result.data)
            dset.attrs.create('stage', result.stage)
            dset.attrs.create('label', result.label)
            dset.attrs.create('story', result.label)

    def read_result(self, experiment_id: int, stage: int):
        filename = self.__get_filename(experiment_id)
        dset_name = self.__get_dset_name(stage)
        # TODO: What if file does not exist?
        with h5py.File(filename, 'r') as file:
            # TODO: What if dset does not exist?
            dset = file.get(dset_name)
            # TODO: strings come back a byte arrays...?
            return dset[()]

    def get_results(
            self,
            # TODO: How to get results across experiments?
            experiment_id: Optional[int] = None,
            label: Optional[str] = None,
            stage: Optional[int] = None,
    ) -> Iterable[Any]:
        """
        Returns a set of results data from HDF5, in the form of a dictionary where the key is the stage and the value
        is the raw data.
        """
        filename = self.__get_filename(experiment_id)
        with h5py.File(filename, 'r') as file:
            # stages = filter(lambda x: x.name.startswith('stage_'), file.keys())
            dsets = map(lambda dset_name: file.get(dset_name), file.keys())
            datas = map(lambda dset: (dset.attrs['stage'], dset[()]), dsets)
            return dict(list(datas))

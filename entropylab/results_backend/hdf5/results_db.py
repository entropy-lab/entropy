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
        dset_name = self.__get_dset_name(result.stage)
        with h5py.File(filename, 'w') as file:
            dset = file.create_dataset(
                name=dset_name,
                data=result.data)
            # TODO: does `label` really belong in an attribute?
            dset.attrs.create('label', result.label)
            # TODO: what do we do with `story`?
            # TODO: strings come back a byte arrays...?

    def read_result(self, experiment_id: int, stage: int):
        filename = self.__get_filename(experiment_id)
        dset_name = self.__get_dset_name(stage)
        # TODO: What if file does not exist?
        with h5py.File(filename, 'r') as file:
            # TODO: What if dset does not exist?
            dset = file.get(dset_name)
            return dset[()]

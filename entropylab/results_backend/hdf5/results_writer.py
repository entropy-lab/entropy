import h5py

from entropylab import RawResultData


class ResultsWriter:

    def __file_session(self):
        return h5py.File(self.path, 'a', self.driver, **self.kwds)

    def __init__(self):
        pass

    def write_result(self, experiment_id: int, result: RawResultData):
        filename = f"experiment_{experiment_id}.hdf5"
        dset_name = f"stage_{result.stage}"
        with h5py.File(filename, 'w') as file:
            dset = file.create_dataset(
                name=dset_name,
                data=result.data)
            # shape=(1,),
            # dtype=type(result.data))
            dset.attrs.create('label', result.label)

import h5py

from entropylab import RawResultData


class ResultsWriter:

    def __init__(self):
        pass

    def write_result(self, experiment_id: int, result: RawResultData):
        filename = f"experiment_{experiment_id}.hdf5"
        dset_name = f"stage_{result.stage}"
        with h5py.File(filename, 'w') as file:
            dset = file.create_dataset(
                name=dset_name,
                data=result.data)
            # TODO: does `label` really belong in an attribute?
            dset.attrs.create('label', result.label)
            # TODO: what do we do with `story`?
            # TODO: strings come back a byte arrays...?

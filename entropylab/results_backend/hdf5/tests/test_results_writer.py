import os
from random import randrange
from typing import Any

import h5py
import numpy as np
import pytest

from entropylab import RawResultData
from entropylab.results_backend.hdf5.results_writer import ResultsWriter


@pytest.mark.parametrize(
    "data", [
        42, True, 3.14159265359, -160000000000000, np.int64(42)
    ])
def test_write_result_with_scalar_data(data: Any):
    filename = ""
    try:
        # arrange
        target = ResultsWriter()
        experiment_id = randrange(10000000)
        result = RawResultData(label="foo", data=data)
        result.stage = randrange(1000)
        result.story = "A long time ago in a galaxy far, far away..."

        # act
        target.write_result(experiment_id, result)

        # assert
        filename = f"experiment_{experiment_id}.hdf5"
        dset_name = f"stage_{result.stage}"
        with h5py.File(filename, 'r') as file:
            dset = file.get(dset_name)
            assert dset[()] == data
    finally:
        # clean up
        os.remove(filename)

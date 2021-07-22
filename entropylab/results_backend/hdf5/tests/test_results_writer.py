import numpy as np

from entropylab import RawResultData
from entropylab.results_backend.hdf5.results_writer import ResultsWriter


def test_write_result_int_data():
    # arrange
    target = ResultsWriter()
    result = RawResultData(label="foo", data=np.arange(1))
    result.stage = 0
    result.story = "A long time ago in a galaxy far, far away..."
    # act
    target.write_result(0, result)
    # assert
    # clean up

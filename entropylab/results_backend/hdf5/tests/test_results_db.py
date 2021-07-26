import os
from random import randrange
from typing import Any

import numpy as np
import pytest

from entropylab import RawResultData
from entropylab.results_backend.hdf5.results_db import ResultsDB


@pytest.mark.parametrize(
    "data", [
        42, True, 3.14159265359, -160000000000000, np.int64(42), "foo",
        [1, 2, 3], np.arange(12)
    ])
def test_write_and_read_single_result(data: Any):
    experiment_id = 0
    target = ResultsDB()
    try:
        # arrange
        experiment_id = randrange(10000000)
        result = RawResultData(label="foo", data=data)
        result.stage = randrange(1000)
        result.story = "A long time ago in a galaxy far, far away..."

        # act
        target.write_result(experiment_id, result)
        actual = target.read_result(experiment_id, result.stage)

        # assert
        if isinstance(data, str):
            assert actual.decode() == data
        elif isinstance(data, list):
            assert_lists_are_equal(actual, data)
        elif isinstance(data, np.ndarray):
            assert_lists_are_equal(actual, data)
        else:
            assert actual == data

    finally:
        # clean up
        filename = target._ResultsDB__get_filename(experiment_id)
        os.remove(filename)


def test_get_results_two_results():
    experiment_id = 0
    target = ResultsDB()
    try:
        # arrange
        experiment_id = randrange(10000000)
        result = RawResultData(label="foo", data=np.arange(12))
        result.stage = 0
        result.story = "A long time ago in a galaxy far, far away..."

        # act
        target.write_result(experiment_id, result)
        result.stage = 1
        target.write_result(experiment_id, result)
        actual = target.get_results(experiment_id, result.stage)

        # assert
        assert len(actual) == 2
    finally:
        # clean up
        filename = target._ResultsDB__get_filename(experiment_id)
        os.remove(filename)


def assert_lists_are_equal(actual, expected):
    assert len(actual) == len(expected)
    assert all([a == b for a, b in zip(actual, expected)])

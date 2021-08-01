import os
from random import randrange
from typing import Any

import h5py
import numpy as np
import pytest

from entropylab import RawResultData
from entropylab.results_backend.hdf5.results_db import ResultsDB, HDF_FILENAME, get_children_or_by_name


@pytest.mark.parametrize(
    "data", [
        42, True, 3.14159265359, -160000000000000, np.int64(42), "foo",
        [1, 2, 3], np.arange(12), ["foo", "bar", "baz"]
    ])
def test_write_and_read_single_result(data: Any):
    target = ResultsDB()
    try:
        # arrange
        experiment_id = randrange(10000000)
        result = RawResultData(label="foo", data=data)
        result.stage = randrange(1000)
        result.story = "A long time ago in a galaxy far, far away..."

        # act
        target.save_result(experiment_id, result)
        actual = target.read_result(experiment_id, result.stage, result.label)

        # assert
        if isinstance(data, list):
            assert_lists_are_equal(actual.data, data)
        elif isinstance(data, np.ndarray):
            assert_lists_are_equal(actual.data, data)
        else:
            assert actual.data == data

    finally:
        # clean up
        # filename = target._ResultsDB__get_filename(experiment_id)
        os.remove(HDF_FILENAME)


def test_get_results_two_results():
    target = ResultsDB()
    try:
        # arrange
        experiment_id = randrange(10000000)
        result = RawResultData(stage=0, label="foo", data=np.arange(12))
        result.story = "A long time ago in a galaxy far, far away..."
        target.save_result(experiment_id, result)
        result2 = RawResultData(stage=0, label="bar", data=np.arange(9))
        result.story = "A long time ago in a galaxy far, far away..."
        target.save_result(experiment_id, result2)

        # act
        actual = target.get_results(experiment_id, result.stage)

        # assert
        assert len(actual) == 2
        assert actual[0].label == "bar"
        assert actual[1].label == "foo"

    finally:
        # clean up
        # filename = target._ResultsDB__get_filename(experiment_id)
        # os.remove(filename)
        os.remove(HDF_FILENAME)


def test_get_last_result_of_experiment():
    target = ResultsDB()
    try:
        # arrange
        experiment_id = randrange(10000000)
        result = RawResultData(stage=0, label="foo", data=np.arange(12))
        target.save_result(experiment_id, result)
        result2 = RawResultData(stage=1, label="bar", data=np.arange(9))
        target.save_result(experiment_id, result2)
        result2 = RawResultData(stage=2, label="bar", data=np.arange(9))
        target.save_result(experiment_id, result2)
        result3 = RawResultData(stage=0, label="bar", data=np.arange(6))
        target.save_result(experiment_id, result3)

        # act
        actual = target.get_last_result_of_experiment(experiment_id)

        # assert
        assert actual.experiment_id == experiment_id
        assert actual.stage == 0
        assert actual.label == "bar"
    finally:
        # clean up
        os.remove(HDF_FILENAME)


def test_get_last_result_of_experiment_when_no_file():
    target = ResultsDB()
    # arrange
    experiment_id = randrange(10000000)
    result = RawResultData(stage=0, label="foo", data=np.arange(12))
    target.save_result(42, result)

    # act
    actual = target.get_last_result_of_experiment(experiment_id)

    # assert
    assert actual is None


def test_get_last_result_of_experiment_when_no_experiment():
    target = ResultsDB()
    try:
        # arrange
        experiment_id = randrange(10000000)

        # act
        actual = target.get_last_result_of_experiment(experiment_id)

        # assert
        assert actual is None
    finally:
        # clean up
        os.remove(HDF_FILENAME)


def test_get_children_or_by_name_when_label_is_not_specified(request):
    filename = f"./{request.node.name}.hdf5"
    try:
        # arrange
        with h5py.File(filename, 'w') as file:
            file.create_dataset("foo", data=42)
            file.create_dataset("bar", data=-3.1412)
            # act
            actual = get_children_or_by_name(file)
            # assert
            assert len(actual) == 2
            names = list(map(lambda d: d.name, actual))
            assert "/foo" in names
            assert "/bar" in names
    finally:
        # clean up
        os.remove(filename)


def test_get_children_or_by_name_when_label_is_specified(request):
    filename = f"./{request.node.name}.hdf5"
    try:
        # arrange
        with h5py.File(filename, 'w') as file:
            file.create_dataset("foo", data=42)
            # act
            actual = get_children_or_by_name(file, "foo")
            # assert
            assert len(actual) == 1
            assert actual[0].name == "/foo"
    finally:
        # clean up
        os.remove(filename)


def test_get_children_or_by_name_when_label_is_not_in_group(request):
    filename = f"./{request.node.name}.hdf5"
    try:
        # arrange
        with h5py.File(filename, 'w') as file:
            file.create_dataset("foo", data=42)
            # act
            actual = get_children_or_by_name(file, "bar")
            # assert
            assert len(actual) == 0
    finally:
        # clean up
        os.remove(filename)


def test_get_children_or_by_name_when_group_is_empty(request):
    filename = f"./{request.node.name}.hdf5"
    try:
        # arrange
        with h5py.File(filename, 'w') as file:
            # act
            actual = get_children_or_by_name(file)
            # assert
            assert len(actual) == 0
    finally:
        # clean up
        os.remove(filename)


def assert_lists_are_equal(actual, expected):
    assert len(actual) == len(expected)
    assert all([a == b for a, b in zip(actual, expected)])

import os
from random import randrange
from typing import Any

import h5py
import numpy as np
import pytest

from entropylab import RawResultData
from entropylab.api.data_writer import Metadata
from entropylab.results_backend.sqlalchemy.storage import (
    HDF5Storage,
    _get_all_or_single,
)
from entropylab.results_backend.sqlalchemy.model import ResultDataType


HDF_FILENAME = "./tests_cache/entropy.hdf5"


class Picklable(object):
    def __init__(self, foo):
        self.foo = foo

    def __eq__(self, obj):
        return isinstance(obj, Picklable) and obj.foo == self.foo


class UnPicklable(object):
    def __init__(self, foo):
        self.foo = foo
        self.baz = lambda: print("You can't pickle me")

    def __eq__(self, obj):
        return isinstance(obj, Picklable) and obj.foo == self.foo


@pytest.mark.parametrize(
    "data",
    [
        42,
        True,
        3.14159265359,
        -160000000000000,
        np.int64(42),
        "foo",
        [1, 2, 3],
        np.arange(12),
        ["foo", "bar", "baz"],
        (42, 2),
        (42, "foo"),
        {"foo": "bar"},
        ResultDataType.String,
        Picklable("bar"),
        UnPicklable("bar"),
        # fixed length unicode string:
        np.array("2.0±0.1".encode("utf-8"), dtype=h5py.string_dtype("utf-8", 30)),
    ],
)
def test_write_and_read_single_result(data: Any):
    target = HDF5Storage(HDF_FILENAME)
    try:
        # arrange
        experiment_id = randrange(10000000)
        result = RawResultData(label="foo", data=data)
        result.stage = randrange(1000)
        result.story = "A long time ago in a galaxy far, far away..."

        # act
        target.save_result(experiment_id, result)
        actual = list(
            target.get_result_records(experiment_id, result.stage, result.label)
        )[0]

        # assert
        if isinstance(data, list) or isinstance(data, tuple):
            _assert_lists_are_equal(actual.data, data)
        elif isinstance(data, np.ndarray):
            assert (actual.data == data).all()
        elif isinstance(data, UnPicklable):
            assert str(actual.data).startswith(
                "<entropylab.results_backend.sqlalchemy.tests.test_storage.UnPicklable"
            )
        else:
            assert actual.data == data

    finally:
        # clean up
        os.remove(HDF_FILENAME)


@pytest.mark.parametrize(
    "data",
    [
        42,
        True,
        3.14159265359,
        -160000000000000,
        np.int64(42),
        "foo",
        [1, 2, 3],
        np.arange(12),
        ["foo", "bar", "baz"],
        (42, 2),
        (42, "foo"),
        {"foo": "bar"},
        ResultDataType.String,
        Picklable("bar"),
        UnPicklable("bar"),
        # fixed length unicode string:
        np.array("2.0±0.1".encode("utf-8"), dtype=h5py.string_dtype("utf-8", 30)),
    ],
)
def test_write_and_read_single_metadata(data: Any):
    target = HDF5Storage(HDF_FILENAME)
    try:
        # arrange
        experiment_id = randrange(10000000)
        stage = randrange(1000)
        metadata = Metadata(label="foo", data=data, stage=stage)

        # act
        target.save_metadata(experiment_id, metadata)
        actual = list(
            target.get_metadata_records(experiment_id, metadata.stage, metadata.label)
        )[0]

        # assert
        if isinstance(data, list) or isinstance(data, tuple):
            _assert_lists_are_equal(actual.data, data)
        elif isinstance(data, np.ndarray):
            assert (actual.data == data).all()
        elif isinstance(data, UnPicklable):
            assert str(actual.data).startswith(
                "<entropylab.results_backend.sqlalchemy.tests.test_storage.UnPicklable"
            )
        else:
            assert actual.data == data

    finally:
        # clean up
        os.remove(HDF_FILENAME)


def test_get_results_two_items():
    target = HDF5Storage(HDF_FILENAME)
    try:
        # arrange
        experiment_id = randrange(10000000)
        result = RawResultData(stage=0, label="foo", data=np.arange(12))
        result.story = "A long time ago in a galaxy far, far away..."
        target.save_result(experiment_id, result)
        result2 = RawResultData(stage=0, label="bar", data=np.arange(9))
        result2.story = "A long time ago in a galaxy far, far away..."
        target.save_result(experiment_id, result2)

        # act
        actual = list(target.get_result_records(experiment_id, result.stage))

        # assert
        assert len(actual) == 2
        assert actual[0].label == "bar"
        assert actual[1].label == "foo"

    finally:
        # clean up
        os.remove(HDF_FILENAME)


def test_get_metadata_two_items():
    target = HDF5Storage(HDF_FILENAME)
    try:
        # arrange
        experiment_id = randrange(10000000)
        result = Metadata(stage=0, label="foo", data=np.arange(12))
        target.save_metadata(experiment_id, result)
        result2 = Metadata(stage=0, label="bar", data=np.arange(9))
        target.save_metadata(experiment_id, result2)

        # act
        actual = list(target.get_metadata_records(experiment_id, result.stage))

        # assert
        assert len(actual) == 2
        assert actual[0].label == "bar"
        assert actual[1].label == "foo"

    finally:
        # clean up
        os.remove(HDF_FILENAME)


def test_get_last_result_of_experiment():
    target = HDF5Storage(HDF_FILENAME)
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
    target = HDF5Storage(HDF_FILENAME)
    # arrange
    experiment_id = randrange(10000000)
    result = RawResultData(stage=0, label="foo", data=np.arange(12))
    target.save_result(42, result)

    # act
    actual = target.get_last_result_of_experiment(experiment_id)

    # assert
    assert actual is None


def test_get_last_result_of_experiment_when_no_experiment():
    target = HDF5Storage(HDF_FILENAME)
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


def test_get_all_or_single_when_label_is_not_specified(request):
    filename = f"./{request.node.name}.hdf5"
    try:
        # arrange
        with h5py.File(filename, "w") as file:
            file.create_dataset("foo", data=42)
            file.create_dataset("bar", data=-3.1412)
            # act
            actual = _get_all_or_single(file)
            # assert
            assert len(actual) == 2
            names = list(map(lambda d: d.name, actual))
            assert "/foo" in names
            assert "/bar" in names
    finally:
        # clean up
        os.remove(filename)


def test_get_all_or_single_when_label_is_specified(request):
    filename = f"./{request.node.name}.hdf5"
    try:
        # arrange
        with h5py.File(filename, "w") as file:
            file.create_dataset("foo", data=42)
            # act
            actual = _get_all_or_single(file, "foo")
            # assert
            assert len(actual) == 1
            assert actual[0].name == "/foo"
    finally:
        # clean up
        os.remove(filename)


def test_get_all_or_single_when_label_is_not_in_group(request):
    filename = f"./{request.node.name}.hdf5"
    try:
        # arrange
        with h5py.File(filename, "w") as file:
            file.create_dataset("foo", data=42)
            # act
            actual = _get_all_or_single(file, "bar")
            # assert
            assert len(actual) == 0
    finally:
        # clean up
        os.remove(filename)


def test_get_all_or_single_when_group_is_empty(request):
    filename = f"./{request.node.name}.hdf5"
    try:
        # arrange
        with h5py.File(filename, "w") as file:
            # act
            actual = _get_all_or_single(file)
            # assert
            assert len(actual) == 0
    finally:
        # clean up
        os.remove(filename)


def _assert_lists_are_equal(actual, expected):
    assert len(actual) == len(expected)
    assert all([a == b for a, b in zip(actual, expected)])

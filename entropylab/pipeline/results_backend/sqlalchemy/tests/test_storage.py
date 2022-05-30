import os
import shutil
from random import randrange
from typing import Any

import h5py
import numpy as np
import pytest

from entropylab import RawResultData
from entropylab.pipeline.api.data_writer import Metadata
from entropylab.conftest import _copy_template
from entropylab.pipeline.results_backend.sqlalchemy.db_initializer import (
    _HDF5_DIRNAME,
    _HDF5_FILENAME,
)
from entropylab.pipeline.results_backend.sqlalchemy.model import ResultDataType
from entropylab.pipeline.results_backend.sqlalchemy.storage import (
    HDF5Storage,
    _get_all_or_single,
)


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


def test_ctor_in_memory():
    HDF5Storage()


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
def test_write_and_read_single_result(data: Any, project_dir_path):
    target = HDF5Storage(project_dir_path)
    # arrange
    experiment_id = randrange(10000000)
    result = RawResultData(label="foo", data=data)
    result.stage = randrange(1000)
    result.story = "A long time ago in a galaxy far, far away..."

    # act
    target.save_result(experiment_id, result)
    actual = list(target.get_result_records(experiment_id, result.stage, result.label))[
        0
    ]

    # assert
    if isinstance(data, list) or isinstance(data, tuple):
        _assert_lists_are_equal(actual.data, data)
    elif isinstance(data, np.ndarray):
        assert (actual.data == data).all()
    elif isinstance(data, UnPicklable):
        assert str(actual.data).startswith(
            "<entropylab.pipeline.results_backend.sqlalchemy.tests.test_storage.UnPicklable"
        )
    else:
        assert actual.data == data


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
def test_write_and_read_single_metadata(data: Any, project_dir_path):
    target = HDF5Storage(project_dir_path)
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
            "<entropylab.pipeline.results_backend.sqlalchemy.tests.test_storage.UnPicklable"
        )
    else:
        assert actual.data == data


def test_get_results_two_items(project_dir_path):
    target = HDF5Storage(project_dir_path)
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


def test_get_metadata_two_items(project_dir_path):
    target = HDF5Storage(project_dir_path)
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


def test_write_and_read_results_from_multiple_experiments(project_dir_path):
    target = HDF5Storage(project_dir_path)
    # arrange
    experiment1_id = randrange(10000000)
    result1 = RawResultData(label="foo", data=42)
    result1.stage = randrange(1000)
    result1.story = "A long time ago in a galaxy far, far away..."

    experiment2_id = experiment1_id + 1
    result2 = RawResultData(label="bar", data="baz")
    result2.stage = randrange(1000)
    result2.story = "’Twas brillig, and the slithy toves..."

    # act
    target.save_result(experiment1_id, result1)
    target.save_result(experiment2_id, result2)
    actual = list(target.get_result_records())

    # assert
    assert actual[0].data == 42
    assert actual[1].data == "baz"


def test_get_last_result_of_experiment(project_dir_path):
    target = HDF5Storage(project_dir_path)
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


def test_migrate_from_global_hdf5_to_per_experiment_hdf5_files(
    project_dir_path, request
):
    hdf5_dir_path = os.path.join(project_dir_path, _HDF5_DIRNAME)
    os.makedirs(hdf5_dir_path, exist_ok=True)
    # copy "per project" hdf5 template into project dir
    per_project_hdf5_file_path = os.path.join(project_dir_path, _HDF5_FILENAME)
    _copy_template(
        "./db_templates/per_project.hdf5", per_project_hdf5_file_path, request
    )
    # target:
    target = HDF5Storage(hdf5_dir_path)

    # act
    target.migrate_from_per_project_hdf5_to_per_experiment_hdf5_files(
        per_project_hdf5_file_path
    )

    # assert
    hdf5_1 = os.path.join(hdf5_dir_path, "1.hdf5")
    with h5py.File(hdf5_1, "r") as file:
        x = file["0/label_61/result"]
        assert x[0, 7] == 6586472
    hdf5_6 = os.path.join(hdf5_dir_path, "6.hdf5")
    with h5py.File(hdf5_6, "r") as file:
        x = file["0/label_363/result"]
        assert x[()] == 482918


def test_get_last_result_of_experiment_when_not_in_file(request):
    path = f"./tests_cache/{request.node.name}"
    if not os.path.exists(path):
        os.mkdir(path)
    target = HDF5Storage(path)
    try:
        # arrange
        experiment_id = randrange(10000000)
        result = RawResultData(stage=0, label="foo", data=np.arange(12))
        target.save_result(42, result)

        # act
        actual = target.get_last_result_of_experiment(experiment_id)

        # assert
        assert actual is None
    finally:
        # clean up
        shutil.rmtree(path)


def test_get_last_result_of_experiment_when_no_experiment(project_dir_path):
    target = HDF5Storage(project_dir_path)
    # arrange
    experiment_id = randrange(10000000)
    # act
    actual = target.get_last_result_of_experiment(experiment_id)
    # assert
    assert actual is None


def test_get_all_or_single_when_label_is_not_specified(project_dir_path):
    filename = os.path.join(project_dir_path, "1.hdf5")
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


def test_get_all_or_single_when_label_is_specified(project_dir_path):
    filename = os.path.join(project_dir_path, "1.hdf5")
    # arrange
    with h5py.File(filename, "w") as file:
        file.create_dataset("foo", data=42)
        # act
        actual = _get_all_or_single(file, "foo")
        # assert
        assert len(actual) == 1
        assert actual[0].name == "/foo"


def test_get_all_or_single_when_label_is_not_in_group(project_dir_path):
    filename = os.path.join(project_dir_path, "1.hdf5")
    # arrange
    with h5py.File(filename, "w") as file:
        file.create_dataset("foo", data=42)
        # act
        actual = _get_all_or_single(file, "bar")
        # assert
        assert len(actual) == 0


def test_get_all_or_single_when_group_is_empty(project_dir_path):
    filename = os.path.join(project_dir_path, "1.hdf5")
    # arrange
    with h5py.File(filename, "w") as file:
        # act
        actual = _get_all_or_single(file)
        # assert
        assert len(actual) == 0


def _assert_lists_are_equal(actual, expected):
    assert len(actual) == len(expected)
    assert all([a == b for a, b in zip(actual, expected)])

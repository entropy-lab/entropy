import os.path

import pytest
from plotly import express as px

from entropylab import SqlAlchemyDB, RawResultData
from entropylab.pipeline.results_backend.sqlalchemy.db_initializer import (
    _ENTROPY_DIRNAME,
    _HDF5_DIRNAME,
)


def test_save_result_raises_when_same_result_saved_twice(initialized_project_dir_path):
    # arrange
    db = SqlAlchemyDB(initialized_project_dir_path)
    raw_result = RawResultData(stage=1, label="foo", data=42)
    db.save_result(0, raw_result)
    with pytest.raises(ValueError):
        # act & assert
        db.save_result(0, raw_result)


def test_get_last_result_of_experiment_when_hdf_is_enabled_then_result_is_from_hdf5(
    initialized_project_dir_path,
):
    # arrange
    db = SqlAlchemyDB(initialized_project_dir_path, enable_hdf5_storage=False)
    db.save_result(1, RawResultData(label="save", data="in db"))
    db = SqlAlchemyDB(initialized_project_dir_path)
    db.save_result(1, RawResultData(label="save", data="in storage"))
    # act
    actual = db.get_last_result_of_experiment(1)
    # assert
    assert actual.data == "in storage"


def test_get_last_result_of_experiment_when_hdf_is_disabled_then_result_is_from_db(
    initialized_project_dir_path,
):
    # arrange
    db = SqlAlchemyDB(initialized_project_dir_path)
    db.save_result(1, RawResultData(label="save", data="in storage"))
    db = SqlAlchemyDB(initialized_project_dir_path, enable_hdf5_storage=False)
    db.save_result(1, RawResultData(label="save", data="in db"))
    # act
    actual = db.get_last_result_of_experiment(1)
    # assert
    assert actual.data == "in db"


def test_save_result_when_successful_then_result_is_saved_to_hdf5_dir(
    initialized_project_dir_path,
):
    # arrange
    db = SqlAlchemyDB(initialized_project_dir_path)
    # act
    db.save_result(1, RawResultData(label="save", data="in storage"))
    # assert
    hdf5_path = os.path.join(
        initialized_project_dir_path, _ENTROPY_DIRNAME, _HDF5_DIRNAME, "1.hdf5"
    )
    assert os.path.isfile(hdf5_path)


def test_save_figure_(initialized_project_dir_path):
    # arrange
    db = SqlAlchemyDB(initialized_project_dir_path)
    figure = px.line(x=["a", "b", "c"], y=[1, 3, 2], title="sample figure")
    # act
    db.save_figure(0, figure)
    # assert
    actual = db.get_figures(0)[0]
    assert actual.figure == figure

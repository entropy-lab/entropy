import os.path
from datetime import datetime

import pytest
from matplotlib import pyplot as plt
from plotly import express as px

from entropylab import SqlAlchemyDB, RawResultData
from entropylab.pipeline.api.data_writer import ExperimentInitialData, ExperimentEndData
from entropylab.pipeline.results_backend.sqlalchemy.db_initializer import (
    _ENTROPY_DIRNAME,
    _HDF5_DIRNAME,
)


def test_save_result_raises_when_same_result_saved_twice(initialized_project_dir_path):
    # arrange
    target = SqlAlchemyDB(initialized_project_dir_path)
    raw_result = RawResultData(stage=1, label="foo", data=42)
    target.save_result(0, raw_result)
    with pytest.raises(ValueError):
        # act & assert
        target.save_result(0, raw_result)


def test_get_last_result_of_experiment_when_hdf_is_enabled_then_result_is_from_hdf5(
    initialized_project_dir_path,
):
    # arrange
    target = SqlAlchemyDB(initialized_project_dir_path, enable_hdf5_storage=False)
    target.save_result(1, RawResultData(label="save", data="in db"))
    target = SqlAlchemyDB(initialized_project_dir_path)
    target.save_result(1, RawResultData(label="save", data="in storage"))
    # act
    actual = target.get_last_result_of_experiment(1)
    # assert
    assert actual.data == "in storage"


def test_get_last_result_of_experiment_when_hdf_is_disabled_then_result_is_from_db(
    initialized_project_dir_path,
):
    # arrange
    target = SqlAlchemyDB(initialized_project_dir_path)
    target.save_result(1, RawResultData(label="save", data="in storage"))
    target = SqlAlchemyDB(initialized_project_dir_path, enable_hdf5_storage=False)
    target.save_result(1, RawResultData(label="save", data="in db"))
    # act
    actual = target.get_last_result_of_experiment(1)
    # assert
    assert actual.data == "in db"


def test_save_result_when_successful_then_result_is_saved_to_hdf5_dir(
    initialized_project_dir_path,
):
    # arrange
    target = SqlAlchemyDB(initialized_project_dir_path)
    # act
    target.save_result(1, RawResultData(label="save", data="in storage"))
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


def test_save_matplotlib_figure_(initialized_project_dir_path):
    # arrange
    db = SqlAlchemyDB(initialized_project_dir_path)
    x = [1, 2, 3, 4]
    y = [10, 40, 20, 30]
    plt.scatter(x, y)
    figure = plt.gcf()
    # act
    db.save_matplotlib_figure(0, figure)
    # assert
    actual = db.get_matplotlib_figures(0)[0]
    assert actual.img_src.startswith(
        "data:image/png;base64,"
        "iVBORw0KGgoAAAANSUhEUgAAAoAAAAHgCAYAAAA10dzkAAAAOXRFWHRTb2Z0d2FyZQBN"
        "YXRwbG90bGliIHZlcnNpb24zLjUuMiwgaHR0cHM6Ly9tYXRwbG90bGliLm9yZy8qNh9F"
    )


def test_get_experiments_range_reads_all_columns():
    # arrange
    target = SqlAlchemyDB()
    initial_data, end_data = __save_one_record_to(target)
    # act
    actual = target.get_experiments_range(0, 1)

    # assert
    record = actual.iloc[-1]
    assert record["id"] == 1
    assert record["label"] == initial_data.label
    assert record["start_time"] == initial_data.start_time
    assert record["end_time"] == end_data.end_time
    assert record["user"] == initial_data.user
    assert record["success"] == end_data.success
    assert not record["favorite"]


@pytest.mark.parametrize("is_favorite", [True, False])
def test_update_experiment_favorite(is_favorite):
    # arrange
    target = SqlAlchemyDB()
    initial_data, end_data = __save_one_record_to(target)

    # act
    target.update_experiment_favorite(1, is_favorite)

    # assert
    actual = target.get_experiments_range(0, 1)
    record = actual.iloc[-1]
    assert record["favorite"] == is_favorite


def __save_one_record_to(target):
    initial_data = ExperimentInitialData(
        label="foo",
        user="bar",
        lab_topology="",
        script="print()",
        start_time=datetime(2022, 2, 22, 14, 22),
        story="baz",
    )
    target.save_experiment_initial_data(initial_data)
    end_data = ExperimentEndData(
        end_time=datetime(2022, 2, 22, 14, 22),
        success=True,
    )
    target.save_experiment_end_data(1, end_data)
    return initial_data, end_data

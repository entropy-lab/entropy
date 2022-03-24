import numpy as np
import pytest
from plotly import graph_objects as go

from entropylab.api.data_reader import FigureRecord
from entropylab.api.errors import EntropyError
from entropylab.results.dashboard.auto_plot import auto_plot


# Dictionaries


def test_auto_plot_empty_dict():
    data = dict()
    with pytest.raises(EntropyError):
        auto_plot(1, data)


def test_auto_plot_dict_with_one_value():
    data = dict(res=[10, 20, 40, 30, 50])
    actual = auto_plot(1, data)
    assert isinstance(actual, FigureRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.figure, go.Figure)


def test_auto_plot_dict_with_two_values():
    data = dict(
        res=[10, 20, 40, 30, 50],
        foo=[-10, -20, -40, -30, -50],
    )
    actual = auto_plot(1, data)
    assert isinstance(actual, FigureRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.figure, go.Figure)


# Lists


def test_auto_plot_list_containing_scalars():
    data = [10, 20, 40, 30, 50]
    actual = auto_plot(1, data)
    assert isinstance(actual, FigureRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.figure, go.Figure)


def test_auto_plot_list_containing_a_list_of_scalars():
    data = [[10, 20, 40, 30, 50]]
    actual = auto_plot(1, data)
    assert isinstance(actual, FigureRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.figure, go.Figure)


def test_auto_plot_list_containing_two_equally_sized_lists():
    data = [[1, 2, 3, 4, 5], [10, 20, 40, 30, 50]]
    actual = auto_plot(1, data)
    assert isinstance(actual, FigureRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.figure, go.Figure)


def test_auto_plot_list_containing_two_differently_sized_lists():
    data = [[1, 2, 3, 4, 5], [10, 20, 40, 30]]
    with pytest.raises(RuntimeError):
        auto_plot(1, data)


def test_auto_plot_list_containing_first_empty_list():
    data = [[], [1]]
    with pytest.raises(RuntimeError):
        auto_plot(1, data)


def test_auto_plot_list_containing_first_second_list():
    data = [[1], []]
    with pytest.raises(RuntimeError):
        auto_plot(1, data)


def test_auto_plot_list_containing_two_empty_lists():
    data = [[], []]
    with pytest.raises(RuntimeError):
        auto_plot(1, data)


def test_auto_plot_2d_lists():
    data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    actual = auto_plot(1, data)
    assert isinstance(actual, FigureRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.figure, go.Figure)


# ndarrays


def test_auto_plot_ndarray_containing_a_list_of_scalars():
    data = np.array([10, 20, 40, 30, 50])
    actual = auto_plot(1, data)
    assert isinstance(actual, FigureRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.figure, go.Figure)


def test_auto_plot_ndarray_containing_a_list_of_with_list_of_scalars():
    data = [[10, 20, 40, 30, 50]]
    actual = auto_plot(1, data)
    assert isinstance(actual, FigureRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.figure, go.Figure)


def test_auto_plot_ndarray_containing_list_with_2_lists():
    data = np.array([[10, 20, 40, 30, 50], [110, 120, 140, 130, 150]])
    actual = auto_plot(1, data)
    assert isinstance(actual, FigureRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.figure, go.Figure)


def test_auto_plot_2d_ndarray():
    data = np.arange(15 ** 2).reshape((15, 15))
    actual = auto_plot(1, data)
    assert isinstance(actual, FigureRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.figure, go.Figure)


def test_auto_plot_number():
    data = 10
    actual = auto_plot(1, data)
    assert isinstance(actual, FigureRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.figure, go.Figure)

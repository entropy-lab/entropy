import numpy as np
import pytest

from entropylab.api.data_reader import PlotRecord
from entropylab.api.plot import CirclePlotGenerator, ImShowPlotGenerator
from entropylab.results.dashboard.auto_plot import auto_plot


# Lists


def test_auto_plot_list_containing_scalars():
    data = [10, 20, 40, 30, 50]
    actual = auto_plot(1, data)
    assert isinstance(actual, PlotRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.generator, CirclePlotGenerator)


def test_auto_plot_list_containing_a_list_of_scalars():
    data = [[10, 20, 40, 30, 50]]
    actual = auto_plot(1, data)
    assert isinstance(actual, PlotRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.generator, CirclePlotGenerator)


def test_auto_plot_list_containing_two_equally_sized_lists():
    data = [[1, 2, 3, 4, 5], [10, 20, 40, 30, 50]]
    actual = auto_plot(1, data)
    assert isinstance(actual, PlotRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.generator, CirclePlotGenerator)


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


def test_auto_plot_list_containing_more_than_two_empty_lists():
    data = [[], [], []]
    with pytest.raises(RuntimeError):
        auto_plot(1, data)


def test_auto_plot_2d_lists():
    data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    actual = auto_plot(1, data)
    assert isinstance(actual, PlotRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.generator, ImShowPlotGenerator)


# ndarrays


def test_auto_plot_ndarray_containing_a_list_of_scalars():
    data = np.array([10, 20, 40, 30, 50])
    actual = auto_plot(1, data)
    assert isinstance(actual, PlotRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.generator, CirclePlotGenerator)


def test_auto_plot_ndarray_containing_a_list_of_with_list_of_scalars():
    data = [[10, 20, 40, 30, 50]]
    actual = auto_plot(1, data)
    assert isinstance(actual, PlotRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.generator, CirclePlotGenerator)


def test_auto_plot_ndarray_containing_list_with_2_lists():
    data = np.array([[10, 20, 40, 30, 50], [110, 120, 140, 130, 150]])
    actual = auto_plot(1, data)
    assert isinstance(actual, PlotRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.generator, CirclePlotGenerator)


def test_auto_plot_2d_ndarray():
    data = np.arange(15 ** 2).reshape((15, 15))
    actual = auto_plot(1, data)
    assert isinstance(actual, PlotRecord)
    assert actual.id == 0
    assert actual.experiment_id == 1
    assert isinstance(actual.generator, ImShowPlotGenerator)

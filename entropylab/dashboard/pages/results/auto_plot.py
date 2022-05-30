from datetime import datetime
from typing import List, Dict

import numpy as np
from plotly import express as px

from entropylab.pipeline.api.data_reader import FigureRecord
from entropylab.pipeline.api.errors import EntropyError


def auto_plot(experiment_id: int, data) -> FigureRecord:
    if isinstance(data, dict):
        return _auto_plot_from_dict(experiment_id, data)
    elif isinstance(data, list):
        plot = _auto_plot_from_list(data)
    elif isinstance(data, np.ndarray):
        plot = _auto_plot_from_ndarray(data)
    elif isinstance(data, int) or isinstance(data, float):
        plot = _auto_plot_from_list([data])
    else:
        raise EntropyError(
            "Only lists, dicts and ndarrays can be auto-plotted at this time"
        )
    plot.experiment_id = experiment_id
    plot.id = 0
    return plot


def _auto_plot_from_dict(experiment_id: int, data: Dict) -> FigureRecord:
    if len(data) > 0:
        first = list(data.values())[0]  # arbitrarily plot "first" value
        return auto_plot(experiment_id, first)
    else:
        raise EntropyError("Cannot auto-plot an empty dict")


def _auto_plot_from_list(data: List) -> FigureRecord:
    if not data:
        raise EntropyError("Cannot auto-plot None")
    if len(data) == 0:
        raise EntropyError("Cannot auto-plot an empty list")
    if _list_is_all_numeric(data):
        return _circle_from_list(data)
    elif _list_contains_one_list_of_scalars(data):
        return _circle_from_list(data[0])
    elif _list_contains_two_lists_of_equal_lengths(data):
        return _circle_from_xy(data[0], data[1])
    else:
        return _imshow_from_2d(data)


def _auto_plot_from_ndarray(data: np.ndarray) -> FigureRecord:
    array: np.ndarray = data
    if not array.any():
        raise EntropyError("Cannot auto-plot an empty ndarray")
    if not np.issubdtype(array.dtype, np.number):
        raise EntropyError("Cannot auto-plot a non-numeric ndarray")
    if _ndarray_is_1d(array):
        return _circle_from_list(array.tolist())
    elif _ndarray_contains_a_single_list_of_scalars(array):
        return _circle_from_list(array[0].tolist())
    elif _ndarray_contains_two_lists_of_equal_lengths(array):
        return _circle_from_xy(array[0], array[1])
    else:
        return _imshow_from_2d(data)


# List helper functions


def _is_numeric(first):
    return isinstance(first, int) or isinstance(first, float)


def _list_is_all_numeric(lst):
    return len(lst) > 0 and all(_is_numeric(x) for x in lst)


def _is_all_lists(lst):
    return all(isinstance(x, List) for x in lst)


def _list_contains_two_lists_of_equal_lengths(data):
    if not _is_all_lists(data):
        raise EntropyError("Auto-plot does not support mixed types")
    if len(data) > 2:
        return False
    if not len(data[0]) == len(data[1]):
        raise EntropyError("Cannot auto-plot 2 sub-lists of unequal lengths")
    if len(data[0]) == 0:
        raise EntropyError("Cannot auto-plot 2 empty sub-lists")
    return True


def _list_contains_one_list_of_scalars(data):
    return (
        len(data) == 1 and isinstance(data[0], list) and _list_is_all_numeric(data[0])
    )


# ndarray helper functions


def _ndarray_is_1d(array):
    return array.ndim == 1 and _list_is_all_numeric(array.tolist())


def _ndarray_contains_a_single_list_of_scalars(array):
    return array.shape[0] == 1 and _list_is_all_numeric(array[0].tolist())


def _ndarray_contains_two_lists_of_equal_lengths(array):
    return (
        array.ndim == 2
        and array.shape[0] == 2
        and _list_is_all_numeric(array[0].tolist())
        and _list_is_all_numeric(array[1].tolist())
    )


def _ndarray_is_2d(array):
    return array.ndim == 2


def _ndarray_is_3d_rgb(array):
    return array.ndim == 3 and array.shape[2] == 3  # Last dim is rgb triples


# helper functions for plotting


def _circle_from_xy(x, y):
    return FigureRecord(
        experiment_id=0, id=0, figure=px.scatter(x=x, y=y), time=datetime.now()
    )


def _xs_from(lst):
    return list(range(len(lst)))


def _circle_from_list(lst):
    x = _xs_from(lst)
    y = lst
    return _circle_from_xy(x, y)


def _imshow_from_2d(data):
    return FigureRecord(
        experiment_id=0, id=0, figure=px.imshow(data), time=datetime.now()
    )

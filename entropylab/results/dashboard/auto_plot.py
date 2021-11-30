from typing import List

import numpy as np
import plotly.graph_objects as go
import plotly.express as px

from entropylab.api.errors import EntropyError
from entropylab.api.plot import CirclePlotGenerator


def auto_plot(data) -> go.Figure:
    if isinstance(data, list):
        if not data:
            raise EntropyError("Cannot auto-plot None")
        if len(data) == 0:
            raise EntropyError("Cannot auto-plot an empty list")
        if list_is_all_numeric(data):
            return circle_from_list(data)
        elif list_contains_one_list_of_scalars(data):
            return circle_from_list(data[0])
        elif list_contains_two_lists_of_equal_lengths(data):
            return circle_from_xy(data[0], data[1])
        elif list_is_2d_equal_dims(data):
            return imshow_from_2d(data)
    elif isinstance(data, np.ndarray):
        array: np.ndarray = data
        if not array.any():
            raise EntropyError("Cannot auto-plot an empty ndarray")
        if not np.issubdtype(array.dtype, np.number):
            raise EntropyError("Cannot auto-plot a non-numeric ndarray")
        if ndarray_is_1d(array):
            return circle_from_list(array.tolist())
        elif ndarray_contains_a_single_list_of_scalars(array):
            return circle_from_list(array[0].tolist())
        elif ndarray_contains_two_lists_of_equal_lengths(array):
            return circle_from_xy(array[0], array[1])
        elif ndarray_is_2d(array):
            return imshow_from_2d(array)
    else:
        raise EntropyError("Only list and ndarrays can be auto-plotted at this time")


def is_numeric(first):
    return isinstance(first, int) or isinstance(first, float)


def xs_from(lst):
    return list(range(len(lst)))


# List helper functions


def list_is_all_numeric(lst):
    return len(lst) > 0 and all(is_numeric(x) for x in lst)


def is_all_lists(lst):
    return all(isinstance(x, List) for x in lst)


def list_contains_two_lists_of_equal_lengths(data):
    if not is_all_lists(data):
        raise EntropyError("Auto-plot does not support mixed types")
    if len(data) > 2:
        return False
    if not len(data[0]) == len(data[1]):
        raise EntropyError("Cannot auto-plot 2 sub-lists of unequal lengths")
    if len(data[0]) == 0:
        raise EntropyError("Cannot auto-plot 2 empty sub-lists")
    return True


def list_contains_one_list_of_scalars(data):
    return len(data) == 1 and isinstance(data[0], list) and list_is_all_numeric(data[0])


def list_is_2d_equal_dims(lst):
    if any(len(sublist) == 0 for sublist in lst):
        raise EntropyError("Cannot auto-plot empty list or lists")
    types = map(type, lst)
    if not all(t == list for t in list(types)):
        raise EntropyError(f"Cannot auto-plot list of these types: {list(types)}")

    sublist_len = len(lst[0])
    return all(
        len(sublist) == sublist_len and list_is_all_numeric(sublist) for sublist in lst
    )


# ndarray helper functions


def ndarray_is_1d(array):
    return array.ndim == 1 and list_is_all_numeric(array.tolist())


def ndarray_contains_a_single_list_of_scalars(array):
    return array.shape[0] == 1 and list_is_all_numeric(array[0].tolist())


def ndarray_contains_two_lists_of_equal_lengths(array):
    return (
        array.ndim == 2
        and array.shape[0] == 2
        and list_is_all_numeric(array[0].tolist())
        and list_is_all_numeric(array[1].tolist())
    )


def ndarray_is_2d(array):
    return array.ndim == 2 and array.shape[0] == array.shape[1]


# helper functions for plotting


def circle_from_xy(x, y):
    figure = go.Figure()
    circle = CirclePlotGenerator()
    circle.plot_plotly(figure, [x, y])
    return figure


def circle_from_list(lst):
    x = xs_from(lst)
    y = lst
    return circle_from_xy(x, y)


def imshow_from_2d(data):
    return px.imshow(data)

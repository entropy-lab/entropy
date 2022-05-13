import numpy
import numpy as np
from plotly import graph_objects as go

from entropylab.results.dashboard.pages.results.callbacks import (
    _copy_aggregate_data_to_clipboard_as_python_code,
)


def test_copy_aggregate_data_to_clipboard_as_python_code_empty_figure():
    figure = go.Figure()
    actual = _copy_aggregate_data_to_clipboard_as_python_code(None, figure.to_dict())
    data = eval(actual.replace("data = ", ""))
    assert data == []


def test_copy_aggregate_data_to_clipboard_as_python_code_empty_values():
    figure = dict(data=[dict(name="", x=[], y=[])])
    actual = _copy_aggregate_data_to_clipboard_as_python_code(None, figure)
    data = eval(actual.replace("data = ", ""))
    assert data is not None
    assert data[0][""]["x"] == []
    assert data[0][""]["y"] == []


def test_copy_aggregate_data_to_clipboard_as_python_code_arrays():
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            mode="lines",
            name="test",
            x=[0, 1, 2, 3, 4],
            y=[5, 6, 7, 8, 9],
        )
    )
    actual = _copy_aggregate_data_to_clipboard_as_python_code(None, figure.to_dict())
    # assert actual == "data = [{'test': {'x': [0, 1, 2, 3, 4], 'y': [0, 1, 2, 3, 4]}}]"
    data = eval(actual.replace("data = ", ""))
    assert data is not None
    assert data[0]["test"]["x"] == [0, 1, 2, 3, 4]
    assert data[0]["test"]["y"] == [5, 6, 7, 8, 9]


def test_copy_aggregate_data_to_clipboard_as_python_code_ndarrays():
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            mode="lines",
            name="test",
            x=np.arange(5),
            y=np.arange(5, 10),
        )
    )
    actual = _copy_aggregate_data_to_clipboard_as_python_code(None, figure.to_dict())
    data = eval(actual.replace("data = ", ""), {"np": numpy})
    assert data is not None
    assert data[0]["test"]["x"].tolist() == [0, 1, 2, 3, 4]
    assert data[0]["test"]["y"].tolist() == [5, 6, 7, 8, 9]

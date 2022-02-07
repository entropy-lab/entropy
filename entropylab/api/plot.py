from typing import List

import plotly
import plotly.express as px
import plotly.graph_objects as go
from bokeh.models import Renderer
from bokeh.plotting import Figure
from matplotlib.axes import Axes as Axes
from matplotlib.figure import Figure as matplotlibFigure

from entropylab.api.data_writer import PlotGenerator


class LinePlotGenerator(PlotGenerator):
    def __init__(self) -> None:
        super().__init__()

    def plot_matplotlib(self, figure: matplotlibFigure, data, **kwargs) -> Renderer:
        raise NotImplementedError()

    def plot_bokeh(self, figure: Figure, data, **kwargs) -> Renderer:
        if isinstance(data, List) and len(data) == 2 and len(data[0]) == len(data[1]):
            x = data[0]
            y = data[1]
            return figure.line(
                x,
                y,
                color=kwargs.get("color", "blue"),
                legend_label=kwargs.get("label", ""),
            )
        else:
            raise TypeError("data type is not supported")

    def plot_plotly(self, figure: plotly.graph_objects.Figure, data, **kwargs):
        if isinstance(data, List) and len(data) == 2 and len(data[0]) == len(data[1]):
            x = data[0]
            y = data[1]
            color = kwargs.pop("color", "blue")
            figure.add_trace(
                go.Scatter(
                    mode="lines",
                    x=x,
                    y=y,
                    line_color=color,
                    **kwargs,
                )
            )
            return figure
        else:
            raise TypeError("data type is not supported")


class CirclePlotGenerator(PlotGenerator):
    def __init__(self) -> None:
        super().__init__()

    def plot_matplotlib(self, figure: Axes, data, **kwargs):
        raise NotImplementedError()

    def plot_bokeh(self, figure: Figure, data, **kwargs) -> Renderer:
        if isinstance(data, List) and len(data) == 2 and len(data[0]) == len(data[1]):
            x = data[0]
            y = data[1]
            color = (kwargs.get("color", "blue"),)
            return figure.circle(
                x,
                y,
                size=10,
                color=color,
                legend_label=kwargs.get("label", ""),
                alpha=0.5,
            )
        else:
            raise TypeError("data type is not supported")

    def plot_plotly(self, figure: plotly.graph_objects.Figure, data, **kwargs):
        if isinstance(data, List) and len(data) == 2 and len(data[0]) == len(data[1]):
            x = data[0]
            y = data[1]
            color = kwargs.pop("color", "blue")
            # noinspection PyTypeChecker
            figure.add_trace(
                go.Scatter(
                    mode="markers",
                    x=x,
                    y=y,
                    marker_color=color,
                    marker_size=10,
                    **kwargs,
                )
            )
            return figure
        else:
            raise TypeError("data type is not supported")


class ImShowPlotGenerator(PlotGenerator):
    def __init__(self) -> None:
        super().__init__()

    def plot_plotly(self, figure: go.Figure, data, **kwargs) -> None:
        headtmap_fig = px.imshow(data)
        figure.add_trace(headtmap_fig.data[0])

        return figure

    def plot_bokeh(self, figure: Figure, data, **kwargs) -> Renderer:
        raise NotImplementedError()

    def plot_matplotlib(self, figure: matplotlibFigure, data, **kwargs):
        raise NotImplementedError()

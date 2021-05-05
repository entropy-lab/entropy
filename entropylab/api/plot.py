from typing import List

from bokeh.models import Renderer
from bokeh.plotting import Figure
from matplotlib.figure import Figure as matplotlibFigure
from matplotlib.axes import Axes as Axes

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


class CirclePlotGenerator(PlotGenerator):
    def __init__(self) -> None:
        super().__init__()

    def plot_matplotlib(self, figure: Axes, data, **kwargs):
        raise NotImplementedError()

    def plot_bokeh(self, figure: Figure, data, **kwargs) -> Renderer:
        if isinstance(data, List) and len(data) == 2 and len(data[0]) == len(data[1]):
            x = data[0]
            y = data[1]
            return figure.circle(
                x,
                y,
                size=10,
                color=kwargs.get("color", "blue"),
                legend_label=kwargs.get("label", ""),
                alpha=0.5,
            )
        else:
            raise TypeError("data type is not supported")

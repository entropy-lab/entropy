from bokeh.models import Renderer
from bokeh.plotting import Figure

from quaentropy.api.data_writer import BokehPlotGenerator, PlotDataType


class BokehLinePlotGenerator(BokehPlotGenerator):
    def __init__(self) -> None:
        super().__init__()

    def plot_in_figure(
        self, figure: Figure, data, data_type: PlotDataType, **kwargs
    ) -> Renderer:
        if data_type == PlotDataType.np_2d:
            # fix the data
            if len(data) != 2:
                data = data.reshape(2, int(data.size / 2))

            x = data[0]
            y = data[1]
        elif data_type == PlotDataType.py_2d:
            x = data[0]
            y = data[1]
        else:
            raise NotImplementedError()
        return figure.line(x, y, color=kwargs.get("color", "blue"),legend_label=kwargs.get('label',''))


class BokehCirclePlotGenerator(BokehPlotGenerator):
    def __init__(self) -> None:
        super().__init__()

    def plot_in_figure(
        self, figure: Figure, data, data_type: PlotDataType, **kwargs
    ) -> Renderer:
        if data_type == PlotDataType.np_2d:
            # fix the data
            if len(data) != 2:
                data = data.reshape(2, int(data.size / 2))

            x = data[0]
            y = data[1]
        elif data_type == PlotDataType.py_2d:
            x = data[0]
            y = data[1]
        else:
            raise NotImplementedError()
        return figure.circle(
            x, y, size=10, color=kwargs.get("color", "blue"), legend_label=kwargs.get('label',''),alpha=0.5
        )

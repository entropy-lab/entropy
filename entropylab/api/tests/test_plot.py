import numpy as np
import plotly

from entropylab.api.plot import CirclePlotGenerator, ImShowPlotGenerator
from plotly.graph_objects import Figure


def test_circle_plot_plotly():
    target = CirclePlotGenerator()
    figure = Figure()
    data = [[0, 1, 2, 3, 4, 5], [0, 1, 2, 3, 4, 5]]
    target.plot_plotly(figure, data)
    i = 0


def test_imshow_plot_plotly():
    target = ImShowPlotGenerator()
    figure = Figure()
    data = np.random.rand(10, 10)
    target.plot_plotly(figure, data)
    assert isinstance(figure.data[0], plotly.graph_objs._heatmap.Heatmap)

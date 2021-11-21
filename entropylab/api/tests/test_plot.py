from entropylab.api.plot import CirclePlotGenerator
from plotly.graph_objects import Figure


def test_circle_plot_plotly():
    target = CirclePlotGenerator()
    figure = Figure()
    data = [[0, 1, 2, 3, 4, 5], [0, 1, 2, 3, 4, 5]]
    target.plot_plotly(figure, data)
    i = 0

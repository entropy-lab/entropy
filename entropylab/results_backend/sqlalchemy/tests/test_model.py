from plotly import express as px
from plotly.io import to_json

from entropylab.results_backend.sqlalchemy.model import FigureTable


class TestFigureTable:
    def test_to_record(self):
        target = FigureTable()
        figure = px.line(x=["a", "b", "c"], y=[1, 3, 2], title="sample figure")
        target.figure = to_json(figure)

        actual = target.to_record()

        assert actual.figure.data[0]["x"] == ("a", "b", "c")
        assert actual.figure.data[0]["y"] == (1, 3, 2)

    def test_from_model(self):
        target = FigureTable()
        figure = px.line(x=["a", "b", "c"], y=[1, 3, 2], title="sample figure")

        actual = target.from_model(1, figure)

        assert actual.experiment_id == 1
        assert actual.figure == to_json(figure)
        assert actual.time is not None

import datetime

from plotly import express as px
from plotly.io import to_json

from entropylab.pipeline.results_backend.sqlalchemy.model import (
    FigureTable,
    MatplotlibFigureTable,
)


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


class TestMatplotlibFigureTable:
    def test_to_record(self):
        target = MatplotlibFigureTable(
            id=42,
            experiment_id=1337,
            img_src="data:image/png;base64,",
            time=datetime.datetime.utcnow(),
        )

        actual = target.to_record()

        assert actual.id == target.id
        assert actual.experiment_id == target.experiment_id
        assert actual.img_src == target.img_src
        assert actual.time == target.time

    def test_from_model(self):
        target = MatplotlibFigureTable()

        actual = target.from_model(1, "data:image/png;base64,")

        assert actual.experiment_id == 1
        assert actual.img_src == "data:image/png;base64,"
        assert actual.time is not None

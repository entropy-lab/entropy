import abc
import datetime as dt
from typing import List

import numpy as np
from pandas import DataFrame

from quaentropy.api.data_reader import PlotRecord
from quaentropy.api.data_writer import PlotDataType
from quaentropy.api.plot import BokehLinePlotGenerator
from quaentropy.results_backend.sqlalchemy.connector import (
    SqlalchemySqlitePandasConnector,
)


class DashboardDataReader(abc.ABC):
    @abc.abstractmethod
    def get_last_experiments(self, number) -> DataFrame:
        pass

    @abc.abstractmethod
    def get_plot_data(self, exp_id: int) -> List[PlotRecord]:
        pass


class MockDashboardDataReader(DashboardDataReader):
    def __init__(self) -> None:
        super().__init__()

    def get_last_experiments(self, number) -> DataFrame:
        return {  # todo update according to columns with real connector
            "graph_id": [np.random.randint(10, 2000) for x in range(number)],
            "x": [np.random.random(10) - 0.5 for x in range(number)],
            "y": [np.random.random(10) - 0.5 for x in range(number)],
            "exp_name": [f"run_{x}" for x in range(number)],
            "date": [dt.datetime.now().strftime("%x")] * number,
            "time": [dt.datetime.now().strftime("%X")] * number,
            "user": ["gal"] * number,
        }

    def get_plot_data(self, exp_id: int) -> List[PlotRecord]:
        # todo create numpy?
        return {
            "x": np.random.random(10) - 0.5,
            "y": np.random.random(10) - 0.5,
            "label": exp_id,
        }


class SqlalchemyDashboardDataReader(DashboardDataReader):
    def __init__(self, connector: SqlalchemySqlitePandasConnector) -> None:
        super().__init__()
        self._db: SqlalchemySqlitePandasConnector = connector

    def get_last_experiments(self, number) -> DataFrame:
        return self._db.get_last_experiments(number)

    def get_plot_data(self, exp_id: int) -> List[PlotRecord]:
        plots = self._db.get_plots(exp_id)

        # do you best otherwise
        if len(plots) == 0:
            result = self._db.get_last_result(exp_id)

            if result:

                data = np.array(result.data)
                plots.append(
                    PlotRecord(
                        experiment_id=exp_id,
                        id=-1,
                        label=f"Automatic {result.label}",
                        story=result.story,
                        plot_data=data,
                        data_type=PlotDataType.np_2d,
                        bokeh_generator=BokehLinePlotGenerator(),
                    )
                )
        return plots

    def freestyle_query(self, query):
        pass
        # todo

import abc
from typing import List

# import numpy as np
from pandas import DataFrame

from entropylab import SqlAlchemyDB
from entropylab.api.data_reader import PlotRecord
from entropylab.results.dashboard.auto_plot import auto_plot


class DashboardDataReader(abc.ABC):
    @abc.abstractmethod
    def get_last_experiments(self, number) -> DataFrame:
        pass

    @abc.abstractmethod
    def get_plot_data(self, exp_id: int) -> List[PlotRecord]:
        pass


class SqlalchemyDashboardDataReader(DashboardDataReader):
    def __init__(self, connector: SqlAlchemyDB) -> None:
        super().__init__()
        self._db: SqlAlchemyDB = connector

    def get_last_experiments(self, number) -> DataFrame:
        return self._db.get_last_experiments(number)

    def get_last_result_of_experiment(self, experiment_id):
        return self._db.get_last_result_of_experiment(experiment_id)

    def get_plot_data(self, exp_id: int) -> List[PlotRecord]:
        plots = self._db.get_plots(exp_id)
        if len(plots) > 0:
            return plots
        else:
            last_result = self._db.get_last_result_of_experiment(exp_id)
            if last_result and last_result.data:
                plot = auto_plot(exp_id, last_result.data)
                return [plot]
            else:
                return []

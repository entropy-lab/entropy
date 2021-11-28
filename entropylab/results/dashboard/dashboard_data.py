import abc
from typing import List

# import numpy as np
from pandas import DataFrame

from entropylab import SqlAlchemyDB
from entropylab.api.data_reader import PlotRecord


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
        # # do you best otherwise
        # if len(plots) == 0:
        #     result = self._db.get_last_result_of_experiment(exp_id)
        #
        #     if result:
        #
        #         data = np.array(result.data)
        #         plots.append(
        #             PlotRecord(
        #                 experiment_id=exp_id,
        #                 id=-1,
        #                 label=f"Automatic {result.label}",
        #                 story=result.story,
        #                 plot_data=data,
        #             )
        #         )
        return plots

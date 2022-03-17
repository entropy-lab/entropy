from __future__ import annotations

import abc
from typing import List, Dict

# import numpy as np
import pandas as pd
from pandas import DataFrame

from entropylab import SqlAlchemyDB
from entropylab.api.data_reader import PlotRecord, FigureRecord
from entropylab.results.dashboard.auto_plot import auto_plot


class DashboardDataReader(abc.ABC):
    @abc.abstractmethod
    def get_last_experiments(
        self, max_num_of_experiments: int, success_filter_value: List[bool]
    ) -> DataFrame:
        pass

    @abc.abstractmethod
    def get_plot_and_figure_data(self, exp_id: int) -> List[PlotRecord]:
        pass


class SqlalchemyDashboardDataReader(DashboardDataReader):
    def __init__(self, connector: SqlAlchemyDB) -> None:
        super().__init__()
        self._db: SqlAlchemyDB = connector

    def get_last_experiments(
        self,
        max_num_of_experiments: int,
        success: bool = None,
    ) -> List[Dict]:
        experiments = self._db.get_last_experiments(max_num_of_experiments, success)
        experiments["success"] = experiments["success"].apply(
            lambda x: "✔️" if x else "❌"
        )
        experiments["start_time"] = pd.DatetimeIndex(
            experiments["start_time"]
        ).strftime("%Y-%m-%d %H:%M:%S")
        experiments["end_time"] = pd.DatetimeIndex(experiments["end_time"]).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        records = experiments.to_dict("records")
        return records

    def get_last_result_of_experiment(
        self,
        experiment_id,
    ):
        return self._db.get_last_result_of_experiment(experiment_id)

    def get_plot_and_figure_data(self, exp_id: int) -> List[PlotRecord | FigureRecord]:
        plots = self._db.get_plots(exp_id)
        figures = self._db.get_figures(exp_id)
        if len(plots) > 0 or len(figures) > 0:
            return [*plots, *figures]
        else:
            # TODO: auto_plot to produce figures, not plots
            last_result = self._db.get_last_result_of_experiment(exp_id)
            if last_result is not None and last_result.data is not None:
                plot = auto_plot(exp_id, last_result.data)
                return [plot]
            else:
                return []

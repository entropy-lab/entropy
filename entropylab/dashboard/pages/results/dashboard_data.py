from __future__ import annotations

import abc
from typing import List, Dict

import pandas as pd

from entropylab import SqlAlchemyDB
from entropylab.dashboard.pages.results.auto_plot import auto_plot
from entropylab.logger import logger
from entropylab.pipeline.api.data_reader import (
    FigureRecord,
    MatplotlibFigureRecord,
)

FAVORITE_TRUE = "⭐"
FAVORITE_FALSE = "✰"
MAX_EXPERIMENTS_NUM = 10000


class DashboardDataReader(abc.ABC):
    @abc.abstractmethod
    def get_last_experiments(
        self,
        max_num_of_experiments: int = MAX_EXPERIMENTS_NUM,
        success: bool = None,
    ) -> List[Dict]:
        pass

    @abc.abstractmethod
    def get_figure_records(
        self, exp_id: int
    ) -> List[FigureRecord | MatplotlibFigureRecord]:
        pass


class DashboardDataWriter(abc.ABC):
    @abc.abstractmethod
    def update_experiment_favorite(self, experiment_id: int, favorite: bool) -> None:
        pass


class SqlalchemyDashboardDataReader(DashboardDataReader, DashboardDataWriter):
    def __init__(self, connector: SqlAlchemyDB) -> None:
        super().__init__()
        self._db: SqlAlchemyDB = connector
        self._figures_cache = {}

    def get_last_experiments(
        self,
        max_num_of_experiments: int = MAX_EXPERIMENTS_NUM,
        success: bool = None,
    ) -> List[Dict]:
        experiments = self._db.get_last_experiments(max_num_of_experiments, success)
        experiments["favorite"] = experiments["favorite"].apply(
            lambda x: FAVORITE_TRUE if x else FAVORITE_FALSE
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

    def get_figure_records(
        self, exp_id: int
    ) -> List[FigureRecord | MatplotlibFigureRecord]:
        # Plotly figures
        if exp_id not in self._figures_cache:
            logger.debug(f"Figures cache miss. exp_id=[{exp_id}]")
            self._figures_cache[exp_id] = self._db.get_figures(exp_id)
        else:
            logger.debug(f"Figures cache hit. exp_id=[{exp_id}]")
        figures = self._figures_cache[exp_id]
        # Matplotlib figures
        # TODO: Cache matplotlib figures?
        matplotlib_figures = self._db.get_matplotlib_figures(exp_id)
        if len(figures) > 0 or len(matplotlib_figures) > 0:
            return [*figures, *matplotlib_figures]
        else:
            last_result = self._db.get_last_result_of_experiment(exp_id)
            if last_result is not None and last_result.data is not None:
                figure = auto_plot(exp_id, last_result.data)
                return [figure]
            else:
                return []

    def update_experiment_favorite(self, experiment_id: int, favorite: bool) -> None:
        self._db.update_experiment_favorite(experiment_id, favorite)

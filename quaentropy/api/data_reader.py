from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Any, Optional

from pandas import DataFrame

from quaentropy.api.data_writer import PlotDataType, BokehPlotGenerator


class ScriptViewer:
    def __init__(self, stages: List[str]) -> None:
        super().__init__()
        self._stages = stages

    def show_stages(self):
        return "\n".join(self._stages)

    def print_stage(self, i):
        return self._stages[i]

    def print_all(self):
        return "\n".join(self._stages)


@dataclass
class ExperimentRecord:
    id: int
    label: str
    script: ScriptViewer
    start_time: datetime
    end_time: Optional[datetime]
    story: str


@dataclass
class MetadataRecord:
    experiment_id: int
    id: int
    label: str
    stage: int
    data: Any


@dataclass
class DebugRecord:
    experiment_id: int
    id: int
    python_env: str
    python_history: str
    station_specs: str
    extra: str


@dataclass
class ResultRecord:
    experiment_id: int
    id: int
    label: str
    story: str
    stage: int
    data: Any


@dataclass
class PlotRecord:
    experiment_id: int
    id: int
    label: str
    story: str
    plot_data: Any = None
    data_type: PlotDataType = PlotDataType.unknown
    bokeh_generator: Optional[BokehPlotGenerator] = None
    label: Optional[str] = None
    story: Optional[str] = None


class DataReader(ABC):
    def __init__(self):
        super().__init__()

    def get_last_experiments(self, count: int) -> DataFrame:
        return self.get_experiments_range(0, count)

    @abstractmethod
    def get_experiments_range(self, starting_from_index: int, count: int) -> DataFrame:
        pass

    @abstractmethod
    def get_experiment_record(self, experiment_id: int) -> Optional[ExperimentRecord]:
        pass

    @abstractmethod
    def get_result(self, experiment_id: int, label: str) -> Optional[ResultRecord]:
        pass

    @abstractmethod
    def get_last_result(self, experiment_id: int) -> Optional[ResultRecord]:
        pass

    @abstractmethod
    def get_metadata_record(
        self, experiment_id: int, label: str
    ) -> Optional[MetadataRecord]:
        pass

    @abstractmethod
    def get_debug_record(self, experiment_id: int) -> Optional[DebugRecord]:
        pass

    @abstractmethod
    def get_plots(self, experiment_id: int) -> List[PlotRecord]:
        pass

    @abstractmethod
    def get_raw_results_from_all_experiments(self, name) -> List[ResultRecord]:
        pass


class SingleExperimentDataReader:
    def __init__(self, experiment_id: int, db: DataReader) -> None:
        super().__init__()
        self._experiment_id = experiment_id
        self._data_reader: DataReader = db

    def get_experiment_data(self) -> ExperimentRecord:
        return self._data_reader.get_experiment_record(self._experiment_id)

    def get_metadata(self, label) -> Optional[MetadataRecord]:
        return self._data_reader.get_metadata_record(self._experiment_id, label)

    def get_debug_data(self) -> Optional[DebugRecord]:
        return self._data_reader.get_debug_record(self._experiment_id)

    def get_raw_results(self, label) -> Optional[ResultRecord]:
        return self._data_reader.get_result(self._experiment_id, label)

    def get_plots(self) -> List[PlotRecord]:
        return self._data_reader.get_plots(self._experiment_id)

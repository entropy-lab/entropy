from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Any, Optional, Iterable

from pandas import DataFrame

from quaentropy.api.data_writer import PlotGenerator


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
    success: bool


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
    plot_data: Any = None
    generator: Optional[PlotGenerator] = None
    label: Optional[str] = None
    story: Optional[str] = None


@dataclass
class NodeResults:
    execution_id: int
    results: Iterable[ResultRecord]


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
    def get_experiments(
        self,
        label: Optional[str] = None,
        start_after: Optional[datetime] = None,
        end_after: Optional[datetime] = None,
        success: Optional[bool] = None,
    ) -> Iterable[ExperimentRecord]:
        pass

    @abstractmethod
    def get_results(
        self,
        experiment_id: Optional[int] = None,
        label: Optional[str] = None,
        stage: Optional[int] = None,
    ) -> Iterable[ResultRecord]:
        pass

    @abstractmethod
    def get_metadata_records(
        self,
        experiment_id: Optional[int] = None,
        label: Optional[str] = None,
        stage: Optional[int] = None,
    ) -> Iterable[MetadataRecord]:
        pass

    @abstractmethod
    def get_last_result_of_experiment(
        self, experiment_id: int
    ) -> Optional[ResultRecord]:
        pass

    @abstractmethod
    def get_debug_record(self, experiment_id: int) -> Optional[DebugRecord]:
        pass

    @abstractmethod
    def get_plots(self, experiment_id: int) -> List[PlotRecord]:
        pass

    @abstractmethod
    def get_nodes_id_by_label(
        self, label: str, experiment_id: Optional[int] = None
    ) -> List[int]:
        pass

    def get_results_from_node(
        self,
        node_label: str,
        experiment_id: Optional[int] = None,
        result_label: Optional[str] = None,
    ) -> Iterable[NodeResults]:
        nodes = self.get_nodes_id_by_label(node_label, experiment_id)
        if not nodes:
            raise KeyError(f"node {node_label} not found")
        nodes_results = []
        for node_id in nodes:
            nodes_results.append(
                NodeResults(
                    node_id,
                    self.get_results(
                        experiment_id,
                        label=result_label,
                        stage=node_id,
                    ),
                )
            )
        return nodes_results


class SingleExperimentDataReader:
    def __init__(self, experiment_id: int, db: DataReader) -> None:
        super().__init__()
        self._experiment_id = experiment_id
        self._data_reader: DataReader = db

    def get_experiment_info(self) -> ExperimentRecord:
        return self._data_reader.get_experiment_record(self._experiment_id)

    def get_debug_record(self) -> Optional[DebugRecord]:
        return self._data_reader.get_debug_record(self._experiment_id)

    def get_metadata_records(
        self, label: Optional[str] = None
    ) -> Iterable[MetadataRecord]:
        return self._data_reader.get_metadata_records(self._experiment_id, label)

    def get_results(self, label: Optional[str] = None) -> Iterable[ResultRecord]:
        return self._data_reader.get_results(self._experiment_id, label)

    def get_plots(self) -> List[PlotRecord]:
        return self._data_reader.get_plots(self._experiment_id)

    def get_nodes_id_by_label(self, label: str):
        return self._data_reader.get_nodes_id_by_label(label, self._experiment_id)

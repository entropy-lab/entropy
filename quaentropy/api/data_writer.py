from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Any, Optional

from bokeh.models import Renderer
from bokeh.plotting import Figure

from quaentropy.instruments.lab_topology import ExperimentTopology


@dataclass
class ExecutionSerializer:
    stages: List[str]

    def serialize(self) -> str:
        return "\n".join([str(stage) for stage in self.stages])


@dataclass
class ExperimentInitialData:
    label: str
    user: str
    lab_topology: ExperimentTopology
    script: ExecutionSerializer
    start_time: datetime
    story: str = None


@dataclass
class ExperimentEndData:
    end_time: datetime
    success: bool
    success_criteria: str


@dataclass
class RawResultData:
    label: str
    data: Any
    stage: int = -1
    story: str = None


@dataclass
class Metadata:
    label: str
    stage: int
    data: Any


@dataclass
class Debug:
    python_env: str
    python_history: str
    station_specs: str
    extra: str


class PlotDataType(Enum):
    unkown = 0
    np_2d = 1
    py_2d = 2


class BokehPlotGenerator(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def plot_in_figure(self, figure: Figure, data, data_type: PlotDataType, **kwargs) -> Renderer:
        pass


@dataclass
class Plot:
    data: Any
    data_type: PlotDataType
    bokeh_generator: Optional[BokehPlotGenerator] = None
    label: Optional[str] = None
    story: Optional[str] = ""


class DataWriter(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def save_experiment_initial_data(self, initial_data: ExperimentInitialData) -> int:
        pass

    @abstractmethod
    def save_experiment_end_data(self, experiment_id: int, end_data: ExperimentEndData):
        pass

    @abstractmethod
    def save_result(self, experiment_id: int, result: RawResultData):
        pass

    @abstractmethod
    def save_metadata(self, experiment_id: int, metadata: Metadata):
        pass

    @abstractmethod
    def save_debug(self, experiment_id: int, debug: Debug):
        pass

    @abstractmethod
    def save_plot(self, experiment_id: int, plot: Plot):
        pass

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Type

from bokeh.models import Renderer
from bokeh.plotting import Figure
from matplotlib.figure import Figure as matplotlibFigure


@dataclass
class ExperimentInitialData:
    label: str
    user: str
    lab_topology: str
    script: str
    start_time: datetime
    story: str = None


@dataclass
class ExperimentEndData:
    end_time: datetime
    success: bool


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


class PlotGenerator(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def plot_bokeh(self, figure: Figure, data, **kwargs) -> Renderer:
        pass

    @abstractmethod
    def plot_matplotlib(self, figure: matplotlibFigure, data, **kwargs):
        pass


@dataclass(frozen=True, eq=True)
class PlotSpec:
    generator: Optional[Type[PlotGenerator]] = None
    label: Optional[str] = None
    story: Optional[str] = ""


@dataclass
class NodeData:
    node_id: int
    start_time: datetime
    label: str
    is_key_node: bool


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
    def save_plot(self, experiment_id: int, plot: PlotSpec, data: Any):
        pass

    @abstractmethod
    def save_node(self, experiment_id: int, node_data: NodeData):
        pass

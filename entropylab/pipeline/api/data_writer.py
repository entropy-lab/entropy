from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import matplotlib
from bokeh.models import Renderer
from bokeh.plotting import Figure
from matplotlib.figure import Figure as matplotlibFigure
from plotly import graph_objects as go


@dataclass
class ExperimentInitialData:
    """
    Information about the experiment that is saved on start
    """

    label: str
    user: str
    lab_topology: str
    script: str
    start_time: datetime
    story: str = None


@dataclass
class ExperimentEndData:
    """
    Information about the experiment that is saved on start
    """

    end_time: datetime
    success: bool


@dataclass
class RawResultData:
    """
    A single result that will be saved
    """

    label: str
    data: Any
    stage: int = -1
    story: str = None

    def __repr__(self):
        return f"<RawResultData(stage='{self.stage}', label='{self.label}')>"


@dataclass
class Metadata:
    """
    A single metadata that will be saved
    """

    label: str
    stage: int
    data: Any

    def __repr__(self):
        return f"<Metadata(stage='{self.stage}', label='{self.label}')>"


@dataclass
class Debug:
    """
    extra information about the experiment execution for better debugging
    """

    python_env: str
    python_history: str
    station_specs: str
    extra: str


class PlotGenerator(ABC):
    """
    An abstract class for plots.
    Implementations of this class will let Entropy to save and view plots.
    Every implementation can either implement all plotting functions
    (within the different environments), or just part of it.
    """

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def plot_bokeh(self, figure: Figure, data, **kwargs) -> Renderer:
        """
            plot the given data within the Bokeh Figure
        :param figure: Bokeh figure to plot in
        :param data: plot data
        :param kwargs: extra parameters for plotting
        """
        pass

    @abstractmethod
    def plot_matplotlib(self, figure: matplotlibFigure, data, **kwargs):
        """
            plot the given data within the matplotlib Figure
        :param figure: matplotlib figure
        :param data: plot data
        :param kwargs: extra parameters for plotting
        """
        pass

    @abstractmethod
    def plot_plotly(self, figure: go.Figure, data, **kwargs) -> None:
        """
            plot the given data within the plot.ly Figure
        :param figure: plot.ly figure
        :param data: plot data
        :param kwargs: extra parameters for plotting
        """
        pass


@dataclass
class NodeData:
    """
    information about a specific node
    """

    stage_id: int
    start_time: datetime
    label: str
    is_key_node: bool


class DataWriter(ABC):
    """
    An abstract class for Entropy database, defines the way entropy saves data
    of experiments.
    """

    def __init__(self):
        super().__init__()

    @abstractmethod
    def save_experiment_initial_data(self, initial_data: ExperimentInitialData) -> int:
        """
        save experiment information to db according to the ExperimentInitialData class
        """
        pass

    @abstractmethod
    def save_experiment_end_data(self, experiment_id: int, end_data: ExperimentEndData):
        """
        save experiment information to db according to the ExperimentEndData class
        """
        pass

    @abstractmethod
    def save_result(self, experiment_id: int, result: RawResultData):
        """
        save a new result to the db according to the RawResultData class
        """
        pass

    @abstractmethod
    def save_metadata(self, experiment_id: int, metadata: Metadata):
        """
        save a new metadata to the db according to the Metadata class
        """
        pass

    @abstractmethod
    def save_debug(self, experiment_id: int, debug: Debug):
        """
        save experiment debug information to the db according to the Debug class
        """
        pass

    def save_figure(self, experiment_id: int, figure: go.Figure) -> None:
        """
            saves a new plotly figure to the db and associates it with an experiment

        :param experiment_id: the id of the experiment to associate the figure to
        :param figure: the figure to save to the database
        """
        pass

    def save_matplotlib_figure(
        self, experiment_id: int, figure: matplotlib.figure.Figure
    ) -> None:
        """
            saves a new matplotlib figure to the db (as a base64-encoded string
            representation of a PNG image) and associates it with an experiment

        :param experiment_id: the id of the experiment to associate the figure to
        :param figure: the figure to save to the database
        """
        pass

    @abstractmethod
    def save_node(self, experiment_id: int, node_data: NodeData):
        """
        saves graph's node data to the db, according to NodeData class
        """
        pass

    @abstractmethod
    def update_experiment_favorite(self, experiment_id: int, favorite: bool) -> None:
        """
        sets the value in the "favorite" column of an 'Experiments' table record
        :param experiment_id: The id of the record to update
        :param favorite: A bool value indicating if the experiment is a favorite
        """
        pass

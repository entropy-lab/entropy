from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Any, Optional, Iterable
from warnings import warn

from pandas import DataFrame
from plotly import graph_objects as go

from entropylab.pipeline.api.data_writer import PlotGenerator


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
    """
    Information about the experiment and the execution
    """

    id: int
    label: str
    script: ScriptViewer
    start_time: datetime
    end_time: Optional[datetime]
    story: str
    success: bool


@dataclass
class MetadataRecord:
    """
    A single record of metadata that was saved during the experiment
    """

    experiment_id: int
    id: str
    label: str
    stage: int
    data: Any
    time: datetime


@dataclass
class DebugRecord:
    """
    extra information about the experiment execution for better debugging
    """

    experiment_id: int
    id: int
    python_env: str
    python_history: str
    station_specs: str
    extra: str


@dataclass
class ResultRecord:
    """
    A single result that was saved during the experiment
    """

    experiment_id: int
    id: str
    label: str
    story: str
    stage: int
    data: Any
    time: datetime


@dataclass
class PlotRecord:
    """
    A single plot information and plotting instructions that was saved during the
    experiment
    """

    experiment_id: int
    id: int
    plot_data: Any = None
    generator: Optional[PlotGenerator] = None
    label: Optional[str] = None
    story: Optional[str] = None


@dataclass
class FigureRecord:
    """
    A single plotly figure that was saved during the experiment
    """

    experiment_id: int
    id: int
    figure: go.Figure
    time: datetime


@dataclass
class NodeResults:
    """
    All results of a specific node
    """

    execution_id: int
    results: Iterable[ResultRecord]


class DataReader(ABC):
    """
    An abstract class for Entropy database, defines the way entropy reads data.
    If a database implements both DataWriter and DataReader abstract classes,
    entropy will be able to read the results with a specific set of functions,
    and view them.
    """

    def __init__(self):
        super().__init__()

    def get_last_experiments(self, count: int, success: bool = None) -> DataFrame:
        """
            read n number of last experiments to a pandas dataframe
        :param count: number of experiments
        :param success: Optional filter for the success property.
        """
        return self.get_experiments_range(0, count, success)

    @abstractmethod
    def get_experiments_range(
        self, starting_from_index: int, count: int, success: bool = None
    ) -> DataFrame:
        """
            read a range of experiments to a pandas dataframe
        :param starting_from_index: experiment index to start from (sorted desc by time)
        :param count: number of experiments
        :param success: Optional filter for the success property.
        :return: A DataFrame containing one row per Experiment
        """
        pass

    @abstractmethod
    def get_experiment_record(self, experiment_id: int) -> Optional[ExperimentRecord]:
        """
            returns a single experiment record, or None if experiment not found
        :param experiment_id: the id of experiment
        """
        pass

    @abstractmethod
    def get_experiments(
        self,
        label: Optional[str] = None,
        start_after: Optional[datetime] = None,
        end_after: Optional[datetime] = None,
        success: Optional[bool] = None,
    ) -> Iterable[ExperimentRecord]:
        """
            get multiple experiments records according to any combination of parameters
            filters
        :param label: experiment label to filter by
        :param start_after: experiments start after specific time
        :param end_after: experiments ended after specific time
        :param success: experiment success criteria
        """
        pass

    @abstractmethod
    def get_results(
        self,
        experiment_id: Optional[int] = None,
        label: Optional[str] = None,
        stage: Optional[int] = None,
    ) -> Iterable[ResultRecord]:
        """
            get multiple results according to any combination of parameters filters

        :param experiment_id: results from specific experiment
        :param label: results label to filter by
        :param stage: results stage within the experiment
        """
        pass

    @abstractmethod
    def get_metadata_records(
        self,
        experiment_id: Optional[int] = None,
        label: Optional[str] = None,
        stage: Optional[int] = None,
    ) -> Iterable[MetadataRecord]:
        """
            get multiple metadata records according to any combination of parameters
            filters

        :param experiment_id: metadata from specific experiment
        :param label: metadata label to filter by
        :param stage: metadata stage within the experiment
        """
        pass

    @abstractmethod
    def get_last_result_of_experiment(
        self, experiment_id: int
    ) -> Optional[ResultRecord]:
        """
        returns the last (by time) results of the requested experiment
        """
        pass

    @abstractmethod
    def get_debug_record(self, experiment_id: int) -> Optional[DebugRecord]:
        """
        returns debug information about the requested experiment
        """
        pass

    # noinspection PyTypeChecker
    @abstractmethod
    def get_plots(self, experiment_id: int) -> List[PlotRecord]:
        """
        returns a list of all plots saved in the requested experiment
        """
        warn(
            "This method will soon be deprecated. Please use get_figures() instead",
            PendingDeprecationWarning,
            stacklevel=2,
        )
        pass

    @abstractmethod
    def get_figures(self, experiment_id: int) -> List[FigureRecord]:
        """
        returns a list of all figures saved in the requested experiment
        """
        pass

    @abstractmethod
    def get_node_stage_ids_by_label(
        self, label: str, experiment_id: Optional[int] = None
    ) -> List[int]:
        """
            returns a list of the stage_id property values of all nodes with the
            given label
        :param label: results label
        :param experiment_id: optional to specify an experiment id, so
                only results from this experiment will be returned
        """
        pass

    def get_results_from_node(
        self,
        node_label: str,
        experiment_id: Optional[int] = None,
        result_label: Optional[str] = None,
    ) -> Iterable[NodeResults]:
        """
            returns multiple results with the given label, and combination of
            parameters filters

        :param node_label: the label of node
        :param experiment_id: optional to specify an experiment id, so
                only results from this experiment will be returned
        :param result_label: optional to specify a label of the results
        """
        node_stage_ids = self.get_node_stage_ids_by_label(node_label, experiment_id)
        if not node_stage_ids:
            raise KeyError(f"node {node_label} not found")
        nodes_results = []
        for stage_id in node_stage_ids:
            nodes_results.append(
                NodeResults(
                    stage_id,
                    self.get_results(
                        experiment_id,
                        label=result_label,
                        stage=stage_id,
                    ),
                )
            )
        return nodes_results


class ExperimentReader:
    """
    Reads results and data from a single experiment
    """

    def __init__(self, experiment_id: int, db: DataReader) -> None:
        """
            Reads results and data from a single experiment
        :param experiment_id: the id of experiment
        :param db: results database that implemented the DataReader abstract class
        """
        super().__init__()
        self._experiment_id = experiment_id
        self._data_reader: DataReader = db

    def get_experiment_info(self) -> ExperimentRecord:
        """
        returns a record of the experiment info
        """
        return self._data_reader.get_experiment_record(self._experiment_id)

    def get_debug_record(self) -> Optional[DebugRecord]:
        """
        returns a record of extra debugging information about the experiment
        """
        return self._data_reader.get_debug_record(self._experiment_id)

    def get_metadata_records(
        self, label: Optional[str] = None
    ) -> Iterable[MetadataRecord]:
        """
            returns an iterable of records with the given label
        :param label: metadata label
        """
        return self._data_reader.get_metadata_records(self._experiment_id, label)

    def get_results(self, label: Optional[str] = None) -> Iterable[ResultRecord]:
        """
            returns an iterable of results record with the given label
        :param label: result label
        """
        return self._data_reader.get_results(self._experiment_id, label)

    def get_plots(self) -> List[PlotRecord]:
        """
        returns a list of plot records that were saved for current experiment
        """
        warn(
            "This method will soon be deprecated. Please use get_plots() instead",
            PendingDeprecationWarning,
            stacklevel=2,
        )
        return self._data_reader.get_plots(self._experiment_id)

    def get_figures(self) -> List[FigureRecord]:
        """
        returns a list of plotly figures that were saved for current experiment
        """
        return self._data_reader.get_figures(self._experiment_id)

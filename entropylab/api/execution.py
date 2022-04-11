import abc
from itertools import count
from typing import Any

from plotly import graph_objects as go

from entropylab.api.data_writer import (
    DataWriter,
    RawResultData,
    Metadata,
    PlotSpec,
)
from entropylab.instruments.lab_topology import ExperimentResources


class EntropyContext:
    """
    An interface for the environment of current execution.
    Using the context, user can save data to the database
        and use resources of the experiments.
    """

    def __init__(
        self,
        exp_id: int,
        data_writer: DataWriter,
        experiment_resources: ExperimentResources,
        stage_id: int,
        context_factory,
    ) -> None:
        """
            An interface for the environment of current execution.
        :param exp_id: experiment id
        :param data_writer: data writer used for this experiment
        :param experiment_resources: the resources used in this experiment
        """
        super().__init__()
        self._data_writer = data_writer
        self._exp_id = exp_id
        self._experiment_resources = experiment_resources
        self._stage_id = stage_id
        self._context_factory = context_factory

    def add_result(self, label: str, data: Any, story: str = None):
        """
        saves a new result from this experiment in the database
        :param label: result label
        :param data: result data
        :param story: story about the result
        """
        self._data_writer.save_result(
            self._exp_id, RawResultData(label, data, self._stage_id, story)
        )

    def add_metadata(self, label: str, metadata: Any):
        """
        saves a new metadata from this experiment in the database
        """
        self._data_writer.save_metadata(
            self._exp_id, Metadata(label, self._stage_id, metadata)
        )

    def add_plot(self, plot: PlotSpec, data: Any):
        """
            saves a new plot from this experiment in the database

        :param plot: description and plotting instructions
        :param data: the data for plotting
        """
        self._data_writer.save_plot(self._exp_id, plot, data)

    def add_figure(self, figure: go.Figure) -> None:
        """
            saves a new figure from this experiment in the database

        :param figure: a Plotly Figure object to be saved
        """
        self._data_writer.save_figure(self._exp_id, figure)

    def get_resource(self, name):
        """
            Return an experiment resource (shared lab resource or execution temp resource).
            User can use this resource within the experiment
        :param name: resource name
        """
        return self._experiment_resources.get_resource(name)

    def has_resource(self, name) -> bool:
        """
            True if the resource exist and was added to the current experiment
        :param name: resource name
        """
        return self._experiment_resources.has_resource(name)

    def _get_stage_id(self):
        return self._stage_id


class _EntropyContextFactory:
    def __init__(
        self,
        exp_id: int,
        data_writer: DataWriter,
        experiment_resources: ExperimentResources,
    ) -> None:
        super().__init__()
        self._data_writer = data_writer
        self._exp_id = exp_id
        self._experiment_resources = experiment_resources
        self._stage_iter = count(start=0, step=1)

    def create(self) -> EntropyContext:
        return EntropyContext(
            self._exp_id,
            self._data_writer,
            self._experiment_resources,
            next(self._stage_iter),
            self,
        )


class ExperimentExecutor(abc.ABC):
    """
    Abstract class for executor,
    Every experiment definition should include an executor.
    on every execution, a new executor will be created.
    """

    def __init__(
        self,
    ) -> None:
        super().__init__()

    @abc.abstractmethod
    def execute(self, entropy_context_factory: _EntropyContextFactory) -> Any:
        """
            actual execution of the experiment
        :param entropy_context_factory: the context of current execution
        """
        pass

    @property
    @abc.abstractmethod
    def failed(self) -> bool:
        """
        :return: True if this execution failed
        """
        pass

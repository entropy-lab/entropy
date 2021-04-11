import abc
from typing import Any

from quaentropy.api.data_reader import SingleExperimentDataReader, DataReader
from quaentropy.api.data_writer import (
    DataWriter,
    RawResultData,
    Metadata,
    Plot,
)
from quaentropy.instruments.lab_topology import ExperimentResources


class EntropyContext:
    def __init__(
        self,
        exp_id: int,
        db: DataWriter,
        used_topology: ExperimentResources,
    ) -> None:
        super().__init__()
        self._data_writer = db
        self._exp_id = exp_id
        self._used_topology = used_topology

    def add_result(self, result: RawResultData):
        self._data_writer.save_result(self._exp_id, result)

    def add_metadata(self, metadata: Metadata):
        self._data_writer.save_metadata(self._exp_id, metadata)

    def add_plot(self, plot: Plot):
        self._data_writer.save_plot(self._exp_id, plot)

    def get_resource(self, name):
        return self._used_topology.get_resource(name)

    def save_instruments_snapshot(self, label: str):
        snapshot = self._used_topology.serialize_resources_snapshot()
        self._data_writer.save_metadata(
            self._exp_id, Metadata(label, 0, snapshot)  # todo remember last stage
        )

    def current_experiment_results(self) -> SingleExperimentDataReader:
        if isinstance(self._data_writer, DataReader):
            return SingleExperimentDataReader(self._exp_id, self._data_writer)
        else:
            raise Exception("database has not implemented data reader interface")


class ExperimentExecutor(abc.ABC):
    def __init__(
        self,
    ) -> None:
        super().__init__()

    @abc.abstractmethod
    def execute(self, runner_context: EntropyContext) -> Any:
        pass

    @property
    @abc.abstractmethod
    def failed(self) -> bool:
        pass

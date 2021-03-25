import abc
from typing import Any, TypeVar, Type

from quaentropy.api.data_reader import SingleExperimentDataReader, DataReader
from quaentropy.api.data_writer import (
    DataWriter,
    RawResultData,
    Metadata,
    Plot,
)
from quaentropy.instruments.lab_topology import ExperimentTopology

InstrumentType = TypeVar("InstrumentType")


class EntropyContext:
    def __init__(
        self, id: int, db: DataWriter, used_topology: ExperimentTopology
    ) -> None:
        super().__init__()
        self._data_writer = db
        self._id = id
        self._used_topology = used_topology

    def add_result(self, result: RawResultData):
        self._data_writer.save_result(self._id, result)

    def add_metadata(self, metadata: Metadata):
        self._data_writer.save_metadata(self._id, metadata)

    def add_plot(self, plot: Plot):
        self._data_writer.save_plot(self._id, plot)

    def get_instrument(self, name) -> InstrumentType:
        return self._used_topology.get(name)

    def save_instruments_snapshot(self, label: str):
        snapshot = self._used_topology.get_snapshot()
        self._data_writer.save_metadata(
            self._id, Metadata(label, 0, snapshot)  # todo remember last stage
        )

    def results_reader(self) -> SingleExperimentDataReader:
        if isinstance(self._data_writer, DataReader):
            return SingleExperimentDataReader(self._id, self._data_writer)
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

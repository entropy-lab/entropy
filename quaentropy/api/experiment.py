import abc
from datetime import datetime
from typing import Optional

from quaentropy.api.data_reader import DataReader
from quaentropy.api.data_reader import SingleExperimentDataReader
from quaentropy.api.data_writer import (
    DataWriter,
    ExperimentInitialData,
    RawResultData,
    ExperimentEndData,
    Metadata,
    ExecutionSerializer,
)
from quaentropy.api.execution import ExperimentExecutor, ExperimentRunningContext
from quaentropy.api.memory_reader_writer import MemoryOnlyDataReaderWriter
from quaentropy.instruments.lab_topology import LabTopology, ExperimentTopology


class Experiment:
    def __init__(self, definition: "ExperimentDefinition", db: DataWriter):
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
        if not isinstance(db, DataWriter):
            raise TypeError(f"db must be of type {DataWriter}")
        self._definition: ExperimentDefinition = definition
        self._data_writer: DataWriter = db
        self._used_topology: ExperimentTopology = (
            definition.get_used_instruments_topology()
        )
        self._user: str = ""
        self._id: int = -1

    def run(self) -> bool:
        self._start_time = datetime.now()
        initial_data = ExperimentInitialData(
            label=self._definition.label,
            user=self._user,
            lab_topology=self._used_topology,
            script=self._definition.get_execution_serializer(),
            start_time=self._start_time,
            story=self._definition.story,
        )
        self._id = self._data_writer.save_experiment_initial_data(initial_data)

        self._used_topology.lock()

        self.save_instruments_snapshot("instruments_start_snapshot")

        executor = self._definition.get_execution_instructions()
        result = executor.execute(
            ExperimentRunningContext(
                id=self._id, db=self._data_writer, used_topology=self._used_topology
            )
        )
        if result:
            self._data_writer.save_result(
                self._id,
                RawResultData(
                    "experiment_result",
                    result,
                    0,
                    story="Final output of the experiment",
                ),
            )

        self.save_instruments_snapshot("instruments_end_snapshot")
        self._used_topology.release()
        self._end_time = datetime.now()

        success = True
        end_data = ExperimentEndData(self._end_time, success, "")
        self._data_writer.save_experiment_end_data(self._id, end_data)
        return success

    def save_instruments_snapshot(self, label: str):
        snapshot = self._used_topology.get_snapshot()
        self._data_writer.save_metadata(
            self._id, Metadata(label, 0, snapshot)
        )

    def results_reader(self) -> SingleExperimentDataReader:
        if isinstance(self._data_writer, DataReader):
            return SingleExperimentDataReader(self._id, self._data_writer)
        else:
            raise Exception("database has not implemented data reader interface")


class ExperimentDefinition(abc.ABC):
    def __init__(
            self,
            topology: Optional[LabTopology],
            label: Optional[str] = None,
            story: str = None,
    ) -> None:
        super().__init__()
        self._topology: LabTopology = topology
        if self._topology is None:
            self._topology = LabTopology()
        self.label = label
        self.story = story

    def get_used_instruments_topology(self) -> ExperimentTopology:
        return ExperimentTopology(self._topology)

    def run(self, db: Optional[DataWriter] = None) -> Experiment:
        if db is None:
            db = MemoryOnlyDataReaderWriter()
        experiment = Experiment(self, db)
        experiment.run()
        return experiment

    @abc.abstractmethod
    def get_execution_instructions(self) -> ExperimentExecutor:
        pass

    @abc.abstractmethod
    def get_execution_serializer(self) -> ExecutionSerializer:
        pass

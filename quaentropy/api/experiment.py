import abc
import json
from datetime import datetime
from typing import Optional

from quaentropy.api.data_reader import DataReader
from quaentropy.api.data_reader import SingleExperimentDataReader
from quaentropy.api.data_writer import (
    DataWriter,
    ExperimentInitialData,
    RawResultData,
    ExperimentEndData,
)
from quaentropy.api.execution import ExperimentExecutor, EntropyContext
from quaentropy.api.memory_reader_writer import MemoryOnlyDataReaderWriter
from quaentropy.instruments.lab_topology import ExperimentResources
from quaentropy.logger import logger


class Experiment:
    def __init__(self, definition: "ExperimentDefinition", db: DataWriter):
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
        if not isinstance(db, DataWriter):
            raise TypeError(f"db must be of type {DataWriter}")
        self._definition: ExperimentDefinition = definition
        self._data_writer: DataWriter = db
        self._experiment_resources: ExperimentResources = (
            definition.get_experiment_resources()
        )
        self._user: str = ""
        self._id: int = -1
        self._executor = self._definition.get_execution_instructions()

    def run(self) -> bool:
        try:
            self._start_time = datetime.now()
            initial_data = ExperimentInitialData(
                label=self._definition.label,
                user=self._user,
                lab_topology=json.dumps(
                    self._experiment_resources._serialize_resources_snapshot()
                ),
                script=self._definition.serialize(self._executor),
                start_time=self._start_time,
                story=self._definition.story,
            )
            self._id = self._data_writer.save_experiment_initial_data(initial_data)

            self._experiment_resources._lock_all_resources()

            result = self._executor.execute(
                EntropyContext(
                    exp_id=self._id,
                    db=self._data_writer,
                    experiment_resources=self._experiment_resources,
                )
            )
            if result:
                self._data_writer.save_result(
                    self._id,
                    RawResultData(
                        "experiment_result",
                        result,
                        -1,
                        story="Final output of the experiment",
                    ),
                )
        finally:
            self._experiment_resources._release_all_resources()

        self._end_time = datetime.now()

        success = True
        end_data = ExperimentEndData(self._end_time, success)
        self._data_writer.save_experiment_end_data(self._id, end_data)
        if self._executor.failed:
            raise RuntimeError("failed to execute entropy experiment")
        logger.info("Finished entropy experiment execution successfully")
        return success

    def results_reader(self) -> SingleExperimentDataReader:
        if isinstance(self._data_writer, DataReader):
            return self._definition.get_data_reader(
                self._id, self._data_writer, self._executor
            )
        else:
            raise Exception("database has not implemented data reader interface")


class ExperimentDefinition(abc.ABC):
    def __init__(
        self,
        resources: Optional[ExperimentResources],
        label: Optional[str] = None,
        story: str = None,
    ) -> None:
        super().__init__()
        self._resources: ExperimentResources = resources
        if self._resources is None:
            self._resources = ExperimentResources()
        self.label = label
        self.story = story
        self._kwargs = {}

    def get_experiment_resources(self) -> ExperimentResources:
        return self._resources

    def run(self, db: Optional[DataWriter] = None, **kwargs) -> Experiment:
        if db is None and (not self._resources or not self._resources.get_results_db()):
            logger.warn(
                f"Results of current execution {self.label} "
                f"will be permanently lost on session close"
            )
            db = MemoryOnlyDataReaderWriter()
        elif db is None:
            db = self._resources.get_results_db()
        self._kwargs = kwargs
        experiment = Experiment(self, db)
        experiment.run()
        return experiment

    @abc.abstractmethod
    def get_execution_instructions(self) -> ExperimentExecutor:
        pass

    @abc.abstractmethod
    def get_data_reader(self, exp_id, db, executor) -> SingleExperimentDataReader:
        pass

    @abc.abstractmethod
    def serialize(self, executor) -> str:
        pass

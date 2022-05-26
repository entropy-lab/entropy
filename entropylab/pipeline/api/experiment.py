import abc
import json
from datetime import datetime
from typing import Optional

from entropylab.pipeline.api.data_reader import DataReader
from entropylab.pipeline.api.data_writer import (
    DataWriter,
    ExperimentInitialData,
    RawResultData,
    ExperimentEndData,
)
from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.api.execution import ExperimentExecutor, _EntropyContextFactory
from entropylab.pipeline.api.memory_reader_writer import MemoryOnlyDataReaderWriter
from entropylab.components.lab_topology import ExperimentResources
from entropylab.logger import logger


class _Experiment:
    """
    Instance of the experiment, executed according to the definition.
    """

    def __init__(self, definition: "ExperimentDefinition", db: DataWriter):
        """
        Instance of the experiment, executed according to the definition.

        :param definition: the experiment definition that includes all relevant
                        information for executing
        :param db: db implementation to save results to
        """
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
        if not isinstance(db, DataWriter):
            raise TypeError(f"db must be of type {DataWriter}")
        self._definition: ExperimentDefinition = definition
        self._data_writer: DataWriter = db
        self._experiment_resources: ExperimentResources = (
            definition.get_experiment_resources()
        )
        if definition._user is None:
            self._user = ""
        else:
            self._user: str = definition._user
        self._id: int = -1
        self._executor = self._definition._get_execution_instructions()

    def run(self) -> bool:
        """
            runs the current experiment
        :return: success
        """
        if self._start_time is not None:
            raise EntropyError("Can not run the same experiment twice")
        try:
            self._start_time = datetime.now()
            initial_data = ExperimentInitialData(
                label=self._definition.label,
                user=self._user,
                lab_topology=json.dumps(
                    self._experiment_resources._serialize_resources_snapshot()
                ),
                script=self._definition.serialize(),
                start_time=self._start_time,
                story=self._definition.story,
            )
            self._id = self._data_writer.save_experiment_initial_data(initial_data)

            self._experiment_resources.start_experiment()
            result = self._executor.execute(
                _EntropyContextFactory(
                    exp_id=self._id,
                    data_writer=self._data_writer,
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
            self._experiment_resources.end_experiment()

        self._end_time = datetime.now()

        success = True
        end_data = ExperimentEndData(self._end_time, success)
        self._data_writer.save_experiment_end_data(self._id, end_data)
        if self._executor.failed:
            raise RuntimeError("failed to execute entropy experiment")
        logger.info("Finished entropy experiment execution successfully")
        return success

    def data_reader(self) -> DataReader:
        """
        Results reader of this experiment instance
        """
        if isinstance(self._data_writer, DataReader):
            return self._data_writer
        else:
            raise Exception("database has not implemented data reader interface")

    @property
    def exp_id(self):
        return self._id


class ExperimentHandle(abc.ABC):
    """
    An handle of the experiment execution
    can be used to get information and read results
    """

    def __init__(self) -> None:
        super().__init__()

    @property
    @abc.abstractmethod
    def id(self):
        """
        experiment execution id
        """
        pass

    @property
    @abc.abstractmethod
    def results(self):
        """
        a reader for the current experiments results
        """
        pass


class ExperimentDefinition(abc.ABC):
    """
    Interface for experiment definitions.
    A definition will include the model and code for the experiment itself,
        resources and extra information.
    A definition that runs within entropy should implement all abstract methods.
    Those methods will be used inside entropy for running, logging, saving
        data and handling resources.
    """

    def __init__(
        self,
        resources: Optional[ExperimentResources],
        label: Optional[str] = None,
        story: str = None,
        user: str = "",
    ) -> None:
        super().__init__()
        self._resources: ExperimentResources = resources
        if self._resources is None:
            self._resources = ExperimentResources()
        self.label = label
        self.story = story
        self._kwargs = {}
        if user is None:
            self._user = ""
        else:
            self._user = user

    def get_experiment_resources(self) -> ExperimentResources:
        """
            experiment resources used in this definition
        :return: experiment resources used in this definition
        :rtype: ExperimentResources
        """
        return self._resources

    def _run(self, db: Optional[DataWriter] = None, **kwargs) -> _Experiment:
        """
            Create a new experiment instance using the current definition,
                and runs it
        :param db: Results db implementation
        :param kwargs: keyword parameters that can be passed to the user code
        :return: The instance of the experiment
        :rtype: _Experiment
        """
        if db is None and (not self._resources or not self._resources.get_results_db()):
            logger.warn(
                f"Results of current execution {self.label} "
                f"will be permanently lost on session close"
            )
            db = MemoryOnlyDataReaderWriter()
        elif db is None:
            db = self._resources.get_results_db()
        self._kwargs = kwargs
        experiment = _Experiment(self, db)
        experiment.run()
        return experiment

    @abc.abstractmethod
    def _get_execution_instructions(self) -> ExperimentExecutor:
        """
        The function should return a subclass of ExperimentExecutor.
        This class holds the information about executing the current type of
            experiment definition
        """
        pass

    @abc.abstractmethod
    def run(self, db: Optional[DataWriter] = None, **kwargs) -> ExperimentHandle:
        """
            Run the experiment in Entropy environment.
            Every call to this function creates a new run and returns a different handle.

        :param db: results db. if given, results will be saved in this DB. otherwise
                results will only be saved during this python session
        :param kwargs: key word arguments that will be passed to the experiment code as well.
                        user can specify here extra arguments, and request them in the
                        functions declarations.
        :return a handle of the new experiment run
        """
        pass

    @abc.abstractmethod
    def serialize(self) -> str:
        """
        Serialize the experiment definition to a string
        This string should be readable, and will be saved on every execution for debugging
            and documentation purposes.
        """
        pass

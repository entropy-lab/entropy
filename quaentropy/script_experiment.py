import inspect
import sys
import traceback
from inspect import signature
from typing import Callable, Any
from typing import Optional

from quaentropy.api.data_reader import SingleExperimentDataReader
from quaentropy.api.data_writer import DataWriter
from quaentropy.api.execution import ExperimentExecutor, EntropyContext
from quaentropy.api.experiment import ExperimentDefinition
from quaentropy.instruments.lab_topology import LabResources, ExperimentResources
from quaentropy.logger import logger


def script_experiment(
    label: str, topology: LabResources = None, db: Optional[DataWriter] = None
):
    def decorate(fn):
        ScriptExperiment(topology, fn, label).run(db)

    return decorate


class ScriptExecutor(ExperimentExecutor):
    def __init__(self, script: Callable) -> None:
        super().__init__()
        self._script: Callable = script
        self._stopped = False

    def execute(self, runner_context: EntropyContext) -> Any:
        try:
            sig = signature(self._script)
            keyword_function_parameters = {}
            for param in sig.parameters:
                if sig.parameters[param].annotation is EntropyContext:
                    keyword_function_parameters[param] = runner_context

            if len(sig.parameters) > 0:
                return self._script(**keyword_function_parameters)
            else:
                return self._script()
        except BaseException as e:
            self._stopped = True
            trace = traceback.format_exception(*sys.exc_info())
            logger.error(f"Stopping Script, Error message: {e} of type {e.__class__.__qualname__}.\ntrace:\n{trace}")
            return

    @property
    def failed(self) -> bool:
        return self._stopped


class ScriptExperiment(ExperimentDefinition):
    def __init__(
        self,
        resources: Optional[ExperimentResources],
        script: Callable,
        label: Optional[str] = None,
        story: str = None,
    ) -> None:
        super().__init__(resources, label, story)
        self._script = script

    def get_execution_instructions(self) -> ExperimentExecutor:
        return ScriptExecutor(self._script)

    def serialize(self, executor) -> str:
        return inspect.getsource(self._script)

    def get_data_reader(self, exp_id, db, executor) -> SingleExperimentDataReader:
        return SingleExperimentDataReader(exp_id, db)

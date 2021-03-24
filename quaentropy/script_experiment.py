import inspect
from inspect import signature
from typing import Callable, Any
from typing import Optional

from quaentropy.api.data_writer import DataWriter, ExecutionSerializer
from quaentropy.api.execution import ExperimentExecutor, ExperimentRunningContext
from quaentropy.api.experiment import ExperimentDefinition
from quaentropy.instruments.lab_topology import LabTopology
from quaentropy.logger import logger


def script_experiment(
    label: str, topology: LabTopology = None, db: Optional[DataWriter] = None
):
    def decorate(fn):
        ScriptExperiment(topology, fn, label).run(db)

    return decorate


class ScriptSerializer(ExecutionSerializer):
    def __init__(self, script: Callable) -> None:
        super().__init__([inspect.getsource(script)])


class ScriptExecutor(ExperimentExecutor):
    def __init__(self, script: Callable) -> None:
        super().__init__()
        self._script: Callable = script
        self._stopped = False

    def execute(self, runner_context: ExperimentRunningContext) -> Any:
        try:
            sig = signature(self._script)
            keyword_function_parameters = {}
            for param in sig.parameters:
                if sig.parameters[param].annotation is ExperimentRunningContext:
                    keyword_function_parameters[param] = runner_context

            if len(sig.parameters) > 0:
                return self._script(**keyword_function_parameters)
            else:
                return self._script()
        except BaseException as e:
            self._stopped = True
            logger.error(f"Stopping Script, Error:", e)
            return

    @property
    def failed(self) -> bool:
        return self._stopped


class ScriptExperiment(ExperimentDefinition):
    def __init__(
        self,
        topology: Optional[LabTopology],
        script: Callable,
        label: Optional[str] = None,
        story: str = None,
    ) -> None:
        super().__init__(topology, label, story)
        self._script = script

    def get_execution_instructions(self) -> ExperimentExecutor:
        return ScriptExecutor(self._script)

    def get_execution_serializer(self) -> ExecutionSerializer:
        return ScriptSerializer(self._script)

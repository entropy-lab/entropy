import inspect
from inspect import signature
from typing import Callable, Any
from typing import Optional

from quaentropy.api.data_writer import DataWriter, ExecutionSerializer
from quaentropy.api.execution import ExperimentExecutor, ExperimentRunningContext
from quaentropy.api.experiment import ExperimentDefinition
from quaentropy.instruments.lab_topology import LabTopology


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

    def execute(self, runner_context: ExperimentRunningContext) -> Any:
        sig = signature(self._script)
        if len(sig.parameters) > 0:
            return self._script(runner_context)
        else:
            return self._script()


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

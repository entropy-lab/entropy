import inspect
import sys
import traceback
from inspect import signature
from typing import Callable, Any
from typing import Optional

from entropylab.api.data_reader import ExperimentReader
from entropylab.api.data_writer import DataWriter
from entropylab.api.execution import (
    ExperimentExecutor,
    EntropyContext,
    _EntropyContextFactory,
)
from entropylab.api.experiment import (
    ExperimentDefinition,
    ExperimentHandle,
    _Experiment,
)
from entropylab.instruments.lab_topology import ExperimentResources
from entropylab.logger import logger


def script_experiment(
    label: str, resources: ExperimentResources = None, db: Optional[DataWriter] = None
):
    """
        decorator for running a script experiment with the given python function
    :param label: experiment label
    :param resources: experiment resources if used in the function
    :param db: results db if used in the function
    :return: the handle of experiment
    """

    def decorate(fn):
        Script(resources, fn, label).run(db)

    return decorate


class _ScriptExecutor(ExperimentExecutor):
    def __init__(self, script: Callable) -> None:
        super().__init__()
        self._script: Callable = script
        self._stopped = False

    def execute(self, entropy_context_factory: _EntropyContextFactory) -> Any:
        try:
            context = entropy_context_factory.create()
            sig = signature(self._script)
            keyword_function_parameters = {}
            for param in sig.parameters:
                if sig.parameters[param].annotation is EntropyContext:
                    keyword_function_parameters[param] = context

            if len(sig.parameters) > 0:
                return self._script(**keyword_function_parameters)
            else:
                return self._script()
        except BaseException as e:
            self._stopped = True
            trace = "\n".join(traceback.format_exception(*sys.exc_info()))
            logger.error(
                f"Stopping Script, Error message: {e} of type "
                f"{e.__class__.__qualname__}.\ntrace:\n{trace}"
            )
            return

    @property
    def failed(self) -> bool:
        return self._stopped


class ScriptExperimentHandle(ExperimentHandle):
    """
    An handle of the script experiment execution
    can be used to get information and read results
    """

    def __init__(self, experiment: _Experiment) -> None:
        super().__init__()
        self._experiment = experiment

    @property
    def id(self):
        return self._experiment.exp_id

    @property
    def results(self) -> ExperimentReader:
        return ExperimentReader(self.id, self._experiment.data_reader())


class Script(ExperimentDefinition):
    """
    Definition of a Script Experiment that gets a python function and run it within Entropy.
    This is the most simple type of experiment, which still handle resources and results,
        but using a simple function.
    """

    def __init__(
        self,
        resources: Optional[ExperimentResources],
        script: Callable,
        label: Optional[str] = None,
        story: str = None,
    ) -> None:
        """
            Script Experiment that gets a python function and run it within Entropy.
        :param resources: shared lab resources or temporary resources
                        that are used in the experiment.
        :param script: the experiment itself in a python function
        :param label: experiment label
        :param story: a description of the experiment, which will create an experiment story
                         with all other parts of the experiment
        """
        super().__init__(resources, label, story)
        self._script = script

    def _get_execution_instructions(self) -> ExperimentExecutor:
        return _ScriptExecutor(self._script)

    def run(self, db: Optional[DataWriter] = None, **kwargs) -> ScriptExperimentHandle:
        experiment = self._run(db, **kwargs)
        return ScriptExperimentHandle(experiment)

    def serialize(self) -> str:
        """
        source code of the given python function
        """
        return inspect.getsource(self._script)

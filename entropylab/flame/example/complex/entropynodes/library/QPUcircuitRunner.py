from entropylab.flame.inputs import Inputs
from entropylab.flame.workflow import Workflow

__all__ = ["QPUcircuitRunner"]


class QPUcircuitRunner(object):
    def __init__(
        self, workflow_node_unique_name, circuit_param=None, error_correction=None
    ):
        """Executes given circuit sequence

        :param circuit_param: (JSON - STREAM) cirucuit description
        :param error_correction: (JSON - STREAM) corrected data
        """
        self._command = "python3"
        self._bin = "circuitExec.py"
        self._name = workflow_node_unique_name
        self._icon = "bootstrap/cpu.svg"
        self._inputs = _Inputs(
            circuit_param=circuit_param,
            error_correction=error_correction,
        )
        self._outputs = _Outputs(self._name)
        self._host = {}
        Workflow._register_node(self)  # register the node in the workflow context

    def host(self, **kwargs):
        """Sets additional options for execution on the host."""
        for key, value in kwargs.items():
            self._host[key] = value
        return self

    @property
    def i(self):
        """Node inputs"""
        return self._inputs

    @property
    def o(self):
        """Node outputs"""
        return self._outputs


class _Inputs(object):
    def __init__(self, circuit_param=None, error_correction=None):
        self._inputs = Inputs()

        self._inputs.state(
            "circuit_param", description="cirucuit description", units="JSON"
        )
        self._inputs.set(circuit_param=circuit_param)

        self._inputs.state(
            "error_correction", description="corrected data", units="JSON"
        )
        self._inputs.set(error_correction=error_correction)

    @property
    def circuit_param(self):
        """Input: cirucuit description (JSON)"""
        return self._inputs.get("circuit_param")

    @circuit_param.setter
    def circuit_param(self, value):
        """Input: cirucuit description (JSON)"""
        self._inputs.set(circuit_param=value)

    @property
    def error_correction(self):
        """Input: corrected data (JSON)"""
        return self._inputs.get("error_correction")

    @error_correction.setter
    def error_correction(self, value):
        """Input: corrected data (JSON)"""
        self._inputs.set(error_correction=value)


class _Outputs(object):
    def __init__(self, name):
        self._name = name
        self._outputs = [
            "precorrected_data",
            "circuit_finished",
            "final_data",
        ]

    @property
    def precorrected_data(self):
        """Output: data send for error correction
        :return: (JSON)
        """
        return "#" + self._name + "/precorrected_data"

    @property
    def circuit_finished(self):
        """Output: Event trigger
        :return: (bool)
        """
        return "#" + self._name + "/circuit_finished"

    @property
    def final_data(self):
        """Output: circuit averaged output
        :return: (bool)
        """
        return "#" + self._name + "/final_data"

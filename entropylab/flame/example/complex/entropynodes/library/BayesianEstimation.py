from entropylab.flame.inputs import Inputs
from entropylab.flame.workflow import Workflow

__all__ = ["BayesianEstimation"]


class BayesianEstimation(object):
    def __init__(self, workflow_node_unique_name, precorrected_data=None):
        """error correction algorithm

        :param precorrected_data: (JSON - STREAM) cirucuit description
        """
        self._command = "python3"
        self._bin = "errorCorrection.py"
        self._name = workflow_node_unique_name
        self._icon = "bootstrap/speedometer.svg"
        self._inputs = _Inputs(
            precorrected_data=precorrected_data,
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
    def __init__(self, precorrected_data=None):
        self._inputs = Inputs()

        self._inputs.state(
            "precorrected_data", description="cirucuit description", units="JSON"
        )
        self._inputs.set(precorrected_data=precorrected_data)

    @property
    def precorrected_data(self):
        """Input: cirucuit description (JSON)"""
        return self._inputs.get("precorrected_data")

    @precorrected_data.setter
    def precorrected_data(self, value):
        """Input: cirucuit description (JSON)"""
        self._inputs.set(precorrected_data=value)


class _Outputs(object):
    def __init__(self, name):
        self._name = name
        self._outputs = [
            "correction_data",
        ]

    @property
    def correction_data(self):
        """Output: corrected data
        :return: (JSON)
        """
        return "#" + self._name + "/correction_data"

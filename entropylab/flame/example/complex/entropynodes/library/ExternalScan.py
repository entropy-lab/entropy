from entropylab.flame.inputs import Inputs
from entropylab.flame.workflow import Workflow

__all__ = ["ExternalScan"]


class ExternalScan(object):
    def __init__(self, workflow_node_unique_name, set_point=None):
        """Sets and maintains instrument variables

        :param set_point: (status - STREAM) node will execute only if it receives positive calibration status
        """
        self._command = "python3"
        self._bin = "externalScan.py"
        self._name = workflow_node_unique_name
        self._icon = "bootstrap/sliders.svg"
        self._inputs = _Inputs(
            set_point=set_point,
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
    def __init__(self, set_point=None):
        self._inputs = Inputs()

        self._inputs.state(
            "set_point",
            description="node will execute only if it receives positive calibration status",
            units="status",
        )
        self._inputs.set(set_point=set_point)

    @property
    def set_point(self):
        """Input: node will execute only if it receives positive calibration status (status)"""
        return self._inputs.get("set_point")

    @set_point.setter
    def set_point(self, value):
        """Input: node will execute only if it receives positive calibration status (status)"""
        self._inputs.set(set_point=value)


class _Outputs(object):
    def __init__(self, name):
        self._name = name
        self._outputs = [
            "setpoint_reached",
        ]

    @property
    def setpoint_reached(self):
        """Output: Trigger
        :return: (bool)
        """
        return "#" + self._name + "/setpoint_reached"

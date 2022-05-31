from entropylab.flame.inputs import Inputs
from entropylab.flame.workflow import Workflow

__all__ = ["TaskList"]


class TaskList(object):
    def __init__(
        self,
        workflow_node_unique_name,
        calibration=None,
        scan_points=None,
        laser_setpoint_locked=None,
        circuit_done=None,
    ):
        """Does scan of parameters and triggers circuit execution

        :param calibration: (status - STATE) node will execute only if it receives positive calibration status
        :param scan_points: (MHz - STATE) an array for setpoint execution
        :param laser_setpoint_locked: (bool - STREAM) Triger when laser setpoint is set
        :param circuit_done: (bool - STREAM) Triger when circuit execution is done
        """
        self._command = "python3"
        self._bin = "sequence.py"
        self._name = workflow_node_unique_name
        self._icon = "bootstrap/list-task.svg"
        self._inputs = _Inputs(
            calibration=calibration,
            scan_points=scan_points,
            laser_setpoint_locked=laser_setpoint_locked,
            circuit_done=circuit_done,
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
    def __init__(
        self,
        calibration=None,
        scan_points=None,
        laser_setpoint_locked=None,
        circuit_done=None,
    ):
        self._inputs = Inputs()

        self._inputs.state(
            "calibration",
            description="node will execute only if it receives positive calibration status",
            units="status",
        )
        self._inputs.set(calibration=calibration)

        self._inputs.state(
            "scan_points", description="an array for setpoint execution", units="MHz"
        )
        self._inputs.set(scan_points=scan_points)

        self._inputs.state(
            "laser_setpoint_locked",
            description="Triger when laser setpoint is set",
            units="bool",
        )
        self._inputs.set(laser_setpoint_locked=laser_setpoint_locked)

        self._inputs.state(
            "circuit_done",
            description="Triger when circuit execution is done",
            units="bool",
        )
        self._inputs.set(circuit_done=circuit_done)

    @property
    def calibration(self):
        """Input: node will execute only if it receives positive calibration status (status)"""
        return self._inputs.get("calibration")

    @calibration.setter
    def calibration(self, value):
        """Input: node will execute only if it receives positive calibration status (status)"""
        self._inputs.set(calibration=value)

    @property
    def scan_points(self):
        """Input: an array for setpoint execution (MHz)"""
        return self._inputs.get("scan_points")

    @scan_points.setter
    def scan_points(self, value):
        """Input: an array for setpoint execution (MHz)"""
        self._inputs.set(scan_points=value)

    @property
    def laser_setpoint_locked(self):
        """Input: Triger when laser setpoint is set (bool)"""
        return self._inputs.get("laser_setpoint_locked")

    @laser_setpoint_locked.setter
    def laser_setpoint_locked(self, value):
        """Input: Triger when laser setpoint is set (bool)"""
        self._inputs.set(laser_setpoint_locked=value)

    @property
    def circuit_done(self):
        """Input: Triger when circuit execution is done (bool)"""
        return self._inputs.get("circuit_done")

    @circuit_done.setter
    def circuit_done(self, value):
        """Input: Triger when circuit execution is done (bool)"""
        self._inputs.set(circuit_done=value)


class _Outputs(object):
    def __init__(self, name):
        self._name = name
        self._outputs = [
            "circuit_specification",
            "scan_setpoint",
            "status",
        ]

    @property
    def circuit_specification(self):
        """Output: Triggers circuit execution by providing information for run
        :return: (JSON)
        """
        return "#" + self._name + "/circuit_specification"

    @property
    def scan_setpoint(self):
        """Output: Laser setpoint
        :return: (MHz)
        """
        return "#" + self._name + "/scan_setpoint"

    @property
    def status(self):
        """Output: window into what node is doing
        :return: (json)
        """
        return "#" + self._name + "/status"

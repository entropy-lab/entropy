from entropylab.flame.inputs import Inputs
from entropylab.flame.workflow import Workflow

__all__ = ["CalibrateSystem"]


class CalibrateSystem(object):
    def __init__(self, workflow_node_unique_name, instrumentAddress=None):
        """Makes sure that system is calibrated

        :param instrumentAddress: (IP address - STATE) instrument IP address for calibration
        """
        self._command = "python3"
        self._bin = "calibrate.py"
        self._name = workflow_node_unique_name
        self._icon = "bootstrap/gear-fill.svg"
        self._inputs = _Inputs(
            instrumentAddress=instrumentAddress,
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
    def __init__(self, instrumentAddress=None):
        self._inputs = Inputs()

        self._inputs.state(
            "instrumentAddress",
            description="instrument IP address for calibration",
            units="IP address",
        )
        self._inputs.set(instrumentAddress=instrumentAddress)

    @property
    def instrumentAddress(self):
        """Input: instrument IP address for calibration (IP address)"""
        return self._inputs.get("instrumentAddress")

    @instrumentAddress.setter
    def instrumentAddress(self, value):
        """Input: instrument IP address for calibration (IP address)"""
        self._inputs.set(instrumentAddress=value)


class _Outputs(object):
    def __init__(self, name):
        self._name = name
        self._outputs = [
            "calibration_status",
        ]

    @property
    def calibration_status(self):
        """Output: Calibration status
        :return: (calibrated, uncalibrated)
        """
        return "#" + self._name + "/calibration_status"

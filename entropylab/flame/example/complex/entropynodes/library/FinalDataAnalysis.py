from entropylab.flame.inputs import Inputs
from entropylab.flame.workflow import Workflow

__all__ = ["FinalDataAnalysis"]


class FinalDataAnalysis(object):
    def __init__(self, workflow_node_unique_name, data=None):
        """Final fitting and plotting

        :param data: (JSON - STREAM) experiment results
        """
        self._command = "python3"
        self._bin = "plot.py"
        self._name = workflow_node_unique_name
        self._icon = "bootstrap/file-bar-graph.svg"
        self._inputs = _Inputs(
            data=data,
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
    def __init__(self, data=None):
        self._inputs = Inputs()

        self._inputs.state("data", description="experiment results", units="JSON")
        self._inputs.set(data=data)

    @property
    def data(self):
        """Input: experiment results (JSON)"""
        return self._inputs.get("data")

    @data.setter
    def data(self, value):
        """Input: experiment results (JSON)"""
        self._inputs.set(data=value)


class _Outputs(object):
    def __init__(self, name):
        self._name = name
        self._outputs = [
            "estimated_parameter",
            "final_plot",
        ]

    @property
    def estimated_parameter(self):
        """Output: final result
        :return: (MHz)
        """
        return "#" + self._name + "/estimated_parameter"

    @property
    def final_plot(self):
        """Output: to-do
        :return: (png)
        """
        return "#" + self._name + "/final_plot"

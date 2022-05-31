from entropylab.flame.inputs import Inputs
from entropylab.flame.workflow import Workflow

__all__ = ["CheerfulNode"]


class CheerfulNode(object):
    def __init__(self, workflow_node_unique_name, customer=None, weather=None):
        """greets customers

        :param customer: (human - STREAM) one person at a time
        :param weather: (best guess - STATE) How is weather today
        """
        self._command = "python3"
        self._bin = "cheerful_node.py"
        self._name = workflow_node_unique_name
        self._icon = "bootstrap/person-circle.svg"
        self._inputs = _Inputs(
            customer=customer,
            weather=weather,
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
    def __init__(self, customer=None, weather=None):
        self._inputs = Inputs()

        self._inputs.state(
            "customer", description="one person at a time", units="human"
        )
        self._inputs.set(customer=customer)

        self._inputs.state(
            "weather", description="How is weather today", units="best guess"
        )
        self._inputs.set(weather=weather)

    @property
    def customer(self):
        """Input: one person at a time (human)"""
        return self._inputs.get("customer")

    @customer.setter
    def customer(self, value):
        """Input: one person at a time (human)"""
        self._inputs.set(customer=value)

    @property
    def weather(self):
        """Input: How is weather today (best guess)"""
        return self._inputs.get("weather")

    @weather.setter
    def weather(self, value):
        """Input: How is weather today (best guess)"""
        self._inputs.set(weather=value)


class _Outputs(object):
    def __init__(self, name):
        self._name = name
        self._outputs = [
            "requested_salary",
        ]

    @property
    def requested_salary(self):
        """Output: requested fees from administrator
        :return: (k$)
        """
        return "#" + self._name + "/requested_salary"

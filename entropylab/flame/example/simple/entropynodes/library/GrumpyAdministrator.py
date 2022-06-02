from entropylab.flame.inputs import Inputs
from entropylab.flame.workflow import Workflow

__all__ = ["GrumpyAdministrator"]


class GrumpyAdministrator(object):
    def __init__(self, workflow_node_unique_name, customers=None, clerk_salary=None):
        """divides task lisk and sends them to others

        :param customers: (list of strings - STATE) all customers we have today
        :param clerk_salary: (k$ - STATE) workforce demands
        """
        self._command = "python3"
        self._bin = "grumpy_administrator.py"
        self._name = workflow_node_unique_name
        self._icon = "bootstrap/person-lines-fill.svg"
        self._inputs = _Inputs(
            customers=customers,
            clerk_salary=clerk_salary,
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
    def __init__(self, customers=None, clerk_salary=None):
        self._inputs = Inputs()

        self._inputs.state(
            "customers",
            description="all customers we have today",
            units="list of strings",
        )
        self._inputs.set(customers=customers)

        self._inputs.state("clerk_salary", description="workforce demands", units="k$")
        self._inputs.set(clerk_salary=clerk_salary)

    @property
    def customers(self):
        """Input: all customers we have today (list of strings)"""
        return self._inputs.get("customers")

    @customers.setter
    def customers(self, value):
        """Input: all customers we have today (list of strings)"""
        self._inputs.set(customers=value)

    @property
    def clerk_salary(self):
        """Input: workforce demands (k$)"""
        return self._inputs.get("clerk_salary")

    @clerk_salary.setter
    def clerk_salary(self, value):
        """Input: workforce demands (k$)"""
        self._inputs.set(clerk_salary=value)


class _Outputs(object):
    def __init__(self, name):
        self._name = name
        self._outputs = [
            "clerk_request",
        ]

    @property
    def clerk_request(self):
        """Output: notifies connected clerk to do work
        :return: (string)
        """
        return "#" + self._name + "/clerk_request"

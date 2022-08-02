from abc import abstractmethod, ABC
from typing import Callable, Dict

from munch import Munch
from qm.QuantumMachinesManager import QuantumMachinesManager
from qualang_tools.config import ConfigBuilder
from qualang_tools.config.parameters import ConfigVars
from qm.QuantumMachinesManager import QuantumMachinesManager
from qualang_tools.config import ConfigurationError

from entropylab.pipeline.api.param_store import ParamStore

class QuAMManager(ABC):
    def __init__(self, path: str, host=None, port=None, **kwargs):
        """
        QuAMManager
        :param host: Host where to find the QM orchestrator. If ``None``,
            local settings are used.
        :type host: str
        :param port: Port where to find the QM orchestrator. If None, local settings are used):
        :type port: int
        :param path: Path of the entropy DB
        :type path: str
        """
        self.path = path
        self.param_store = InProcessParamStore(path)
        self._config_builder = ConfigBuilder()
        self._config_vars = ConfigVars()
        self.config = {}
        self.host = host
        self.port = port
        self.qmm = QuantumMachinesManager(host=host, port=port, **kwargs)

    def generate_config(self):
        """Returns the QUA configuration"""
        self._config_builder = ConfigBuilder()
        self.prepare_config(self._config_builder)
        self._set_config_vars()
        self.config = self._config_builder.build()
        return self.config

    @property
    def config_builder(self):
        return self._config_builder

    @abstractmethod
    def prepare_config(self, cb: ConfigBuilder) -> None:
        """method to prepare the ConfigBuilder, needs to be implemented by the QuAMManager"""
        pass

    def parameter(self, var: str, setter: Callable = None):
        """Returns a parameter with the given name and setter, all parameters are saved
        in the ParamStore
        :return: a parameter
        :rtype: Parameter
        """
        if var not in self.param_store.keys():
            self.param_store[var] = None
        return self._config_vars.parameter(var, setter=setter)

    def _set_config_vars(self):
        """Sets the parameter values according to the key, value pairs in the param store"""
        _dict = {}
        for k in self.param_store.keys():
            if not callable(self._config_vars.parameter(k)._value):
                if self.param_store[k] is None:
                    raise ConfigurationError("Set parameter {}".format(k))
                _dict[k] = self.param_store[k]
        self._config_vars.set(**_dict)

    @property
    def elements(self):
        """Returns a Munch instance of element keys available in the QUA configuration"""
        return get_key("elements", self.config)

    @property
    def pulses(self):
        """Returns a Munch instance of pulses available in the QUA configuration"""
        return get_key("pulses", self.config)

    @property
    def integration_weights(self):
        """Returns a Munch instance of integration weights available in the QUA configuration"""
        return get_key("integration_weights", self.config)

    def open_qm(self):
        return self.qmm.open_qm(self.generate_config())


def dict_keys_to_munch(d: Dict):
    elms = Munch()
    for k in d.keys():
        elms[k] = k
    return elms


def get_key(k: str, d: dict):
    if k in d:
        return dict_keys_to_munch(d[k])
    else:
        raise KeyError(
            "{} doesn't exist in the QUA configuration, probably the \
                config wasn't generated".format(
                k
            )
        )

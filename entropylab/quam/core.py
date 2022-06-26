from abc import abstractmethod
from typing import Callable, Dict
from munch import Munch

from entropylab.pipeline.api.in_process_param_store import InProcessParamStore
from qualang_tools.config import ConfigBuilder
from qualang_tools.config.parameters import ConfigVars
from qm.QuantumMachinesManager import QuantumMachinesManager


class QuAMCore(object):
    def __init__(self, path: str):
        """Core QUAM object
        :param path: Path of the entropy DB
        :type path: str
        """
        self.path = path
        self.param_store = InProcessParamStore(path)


class QuAMManager(QuAMCore):
    def __init__(self, path: str, **qmm_kwargs):
        """Admin
        :param path: Path of the entropy DB
        :type path: str
        """
        super().__init__(path=path)
        self._qmm_kwargs = qmm_kwargs
        self._config_builder = ConfigBuilder()
        self._config_vars = ConfigVars()

    def generate_config(self):
        """Returns the QUA configuration"""
        self._config_builder = ConfigBuilder()
        self.prepare_config(self._config_builder)
        self._set_config_vars()
        return self._config_builder.build()

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
                _dict[k] = self.param_store[k]
        self._config_vars.set(**_dict)

    def open_quam(self):
        """Returns an instance of QuAM
        :return: a quam
        :rtype: QuAM
        """
        self._set_config_vars()
        return QuAM(path=self.path, config=self.generate_config(), **self._qmm_kwargs)


class QuAM(QuAMCore):
    def __init__(self, path: str, config: dict = None, **qmm_kwargs):
        """User class to facilitate writing and execution of QUA programs
        :param path: path to the entropy DB
        :type path: str
        :param config: a QUA configuration, defaults to an empty dictionary
        :type config: Dict, optional
        """
        super().__init__(path=path)
        self.config = config if config is not None else {}
        self.qmm = QuantumMachinesManager(**qmm_kwargs)

    @property
    def elements(self):
        """Returns a Munch instance of element keys available in the QUA configuration"""
        return dict_keys_to_munch(self.config["elements"])

    @property
    def pulses(self):
        """Returns a Munch instance of pulses available in the QUA configuration"""
        return dict_keys_to_munch(self.config["pulses"])

    @property
    def integration_weights(self):
        """Returns a Munch instance of integration weights available in the QUA configuration"""
        return dict_keys_to_munch(self.config["integration_weights"])


def dict_keys_to_munch(d: Dict):
    elms = Munch()
    for k in d.keys():
        elms[k] = k
    return elms

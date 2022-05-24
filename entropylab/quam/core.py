from abc import abstractmethod
import copy
from typing import Callable, Dict
from munch import Munch

from entropylab.api.in_process_param_store import InProcessParamStore

from qualang_tools.config import ConfigBuilder
from qualang_tools.config.parameters import ConfigVars

from qm.qua import program
from qm.QuantumMachinesManager import QuantumMachinesManager


class QuamCore(object):
    def __init__(self, path: str):
        """Core QUAM object
        :param path: Path of the entropy DB
        :type path: str
        """
        self.path = path
        self.params = InProcessParamStore(path)

    @property
    def parameters(self):
        """Returns a list of all parameters in the params DB.
        :return: list of parameters
        :rtype: List
        """
        return list(self.params.keys())


class Admin(QuamCore):
    def __init__(self, path: str, host: str = "127.0.0.1"):
        """Admin
        :param path: Path of the entropy DB
        :type path: str
        :param host: QM manager url
        :type host: str
        """
        super().__init__(path=path)
        self.host = host
        self._config_builder = ConfigBuilder()
        self._c_vars = ConfigVars()

    @property
    def config(self):
        """Returns the QUA configuration"""
        self._config_builder = ConfigBuilder()
        self.prepare_config(self._config_builder)
        return self._config_builder.build()

    @abstractmethod
    def prepare_config(self, cb: ConfigBuilder) -> None:
        """method to prepare the ConfigBuilder, needs to be implemented by the admin"""
        pass

    def parameter(self, var: str, setter: Callable = None):
        """Returns a parameter with the given name and setter, all parameters are saved
        in the ParamStore
        :return: a parameter
        :rtype: Parameter
        """
        if var not in self.params.keys():
            self.params[var] = None
        return self._c_vars.parameter(var)

    def set(self, **kwargs):
        """Sets the parameter values according to the key, value pairs in the dictionary"""
        for (k, v) in kwargs.items():
            if not callable(v):
                self.params[k] = v
            else:
                pass
        self._c_vars.set(**kwargs)

    def get_oracle(self, commit_id: str = None):
        """Returns an instance of Oracle
        :return: an oracle
        :rtype: Oracle
        """
        self._set_params()
        return Oracle(path=self.path, config=self.config)

    def get_user(self):
        """Returns an instance of User
        :return: an user
        :rtype: User
        """
        self._set_params()
        return User(path=self.path, config=self.config)

    def _set_params(self):
        _dict = {}
        for k in self.params:
            _dict[k] = self.params[k]
        self.set(**_dict)


class Oracle(QuamCore):
    def __init__(self, path: str, config: dict):
        """Oracle class for querying the list of elements, waveforms, pulses etc.
        prepared by the admin.
        :param path: path to the entropy DB
        :type path: str
        :param config: QUA configuration dictionary
        :type config: dict
        """
        super().__init__(path=path)
        self.config = config

    @property
    def elements(self):
        """Returns a list of quantum elements in the QUA configuration
        :return: list of quantum elements
        :rtype: List
        """
        return list(self.config["elements"].keys())

    @property
    def waveforms(self):
        """Returns a list of waveforms in the QUA configuration
        :return: list of waveforms
        :rtype: List
        """
        return list(self.config["waveforms"].keys())

    @property
    def pulses(self):
        """Returns a list of waveforms in the QUA configuration
        :return: list of waveforms
        :rtype: List
        """
        return list(self.config["pulses"].keys())

    @property
    def controllers(self):
        """Returns a list of controllers in the QUA configuration
        :return: list of controllers
        :rtype: List
        """
        return list(self.config["controllers"].keys())

    @property
    def integration_weights(self):
        """Returns a list of integration weights in the QUA configuration
        :return: list of integration weights
        :rtype: List
        """
        return list(self.config["integration_weights"].keys())


class User(QuamCore):
    def __init__(self, path: str, config: dict = None, host: str = "127.0.0.1"):
        """User class to facilitate writing and execution of QUA programs
        :param path: path to the entropy DB
        :type path: str
        :param config: a QUA configuration, defaults to an empty dictionary
        :type config: Dict, optional
        :param host: host url for Quantum Machine Manager, defaults to local host
        :type host: str, optional
        """
        super().__init__(path=path)
        self.config = config if config is not None else {}
        self._config = copy.deepcopy(config)
        self.host = host
        self.qm = QuantumMachinesManager(host).open_qm(config)

    def simulate(self, prog: program, simulation_config=None):
        """Simulate the given QUA program
        :param prog: a QUA program
        :type prog: program
        :param simulation_config: simulation configuration, defaults to None
        :type simulation_config: SimulationConfig
        """
        self._open_qm()
        job = self.qm.simulate(prog, simulation_config)
        job.result_handles.wait_for_all_values()
        return job

    def execute(self, prog: program, simulation_config=None):
        """Execute the given QUA program on OPX
        :param prog: a QUA program
        :type prog: program
        :param simulation_config: simulation configuration, defaults to None
        :type simulation_config: SimulationConfig
        """
        self._open_qm()
        job = self.qm.execute(prog, simulation_config)
        job.result_handles.wait_for_all_values()
        return job

    def _open_qm(self):
        if self._config != self.config:
            self.qm = self.QuantumMachinesManager(self.host).open_qm(self.config)
            self._config = copy.deepcopy(self.config)

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

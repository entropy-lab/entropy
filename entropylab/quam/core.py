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
    def __init__(self, path):
        self.path = path
        self.params = InProcessParamStore(path)

    @property
    def parameters(self):
        return list(self.params.keys())

class Admin(QuamCore):
    def __init__(self, path: str, host: str = "127.0.0.1"):
        super().__init__(path=path)
        self.host = host
        self._config_builder = ConfigBuilder()
        self._c_vars = ConfigVars()

    @property
    def config(self):
        self._config_builder = ConfigBuilder()
        self.prepare_config(self._config_builder)
        return self._config_builder.build()

    @abstractmethod
    def prepare_config(self, cb: ConfigBuilder) -> None:
        pass

    def parameter(self, var: str, setter: Callable = None):
        if var not in self.params.keys():
            self.params[var] = None
        return self._c_vars.parameter(var)

    def set(self, **kwargs):
        for (k, v) in kwargs.items():
            if not callable(v):
                self.params[k] = v
            else:
                pass
        self._c_vars.set(**kwargs)

    def get_oracle(self, commit_id: str = None):
        return Oracle(path=self.path, config=self.config)

    def get_user(self):
        return User(path=self.path, config=self.config)


class Oracle(QuamCore):
    def __init__(self, path: str, config: dict):
        super().__init__(path=path)
        self.config = config

    @property
    def elements(self):
        return list(self.config["elements"].keys())

    @property
    def waveforms(self):
        return list(self.config["waveforms"].keys())

    @property
    def pulses(self):
        return list(self.config["pulses"].keys())

    @property
    def controllers(self):
        return list(self.config["controllers"].keys())

    @property
    def integration_weights(self):
        return list(self.config["integration_weights"].keys())


class User(QuamCore):
    def __init__(self, path: str, config: dict = {}, host: str = "127.0.0.1"):
        super().__init__(path=path)
        self.config = config
        self._config = copy.deepcopy(config)
        self.host = host
        self.qm = QuantumMachinesManager(host).open_qm(config)

    def simulate(self, prog: program, simulation_config=None):
        self._open_qm()
        job = self.qm.simulate(prog, simulation_config)
        job.result_handles.wait_for_all_values()
        return job

    def execute(self, prog: program, simulation_config=None):
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
        return dict_to_munch(self.config["elements"])

    @property
    def pulses(self):
        return dict_to_munch(self.config["pulses"])

    @property
    def integration_weights(self):
        return dict_to_munch(self.config["integration_weights"])


def dict_to_munch(d: Dict):
    elms = Munch()
    for k in d.keys():
        elms[k] = k
    return elms
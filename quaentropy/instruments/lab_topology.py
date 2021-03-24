import abc
import inspect
from dataclasses import dataclass
from typing import TypeVar, Type, Optional, Dict, Any

from quaentropy.api.errors import DriverNotFound
from quaentropy.instruments.instrument_driver import Driver
from quaentropy.logger import logger

T = TypeVar("T")


class LabTopologyBackend(abc.ABC):
    def __init__(self) -> None:
        super().__init__()

    @abc.abstractmethod
    def save_driver(self, name: str, driver_source_code: str, type_name: str):
        pass

    @abc.abstractmethod
    def save_state(self, name: str, state: str):
        pass

    @abc.abstractmethod
    def get_latest_state(self, name) -> str:
        pass

    @abc.abstractmethod
    def get_driver_code(self, name) -> str:
        pass

    @abc.abstractmethod
    def get_type_name(self, name) -> str:
        pass


@dataclass
class _LabTopologyDevice:
    name: str
    type: Optional[Type]
    args: Optional[Any]
    instance: Optional[Driver] = None


class LabTopology:
    def __init__(self, backend: Optional[LabTopologyBackend] = None) -> None:
        super().__init__()
        self._drivers: Dict[str, _LabTopologyDevice] = {}
        self._backend = backend

    def get(self, name: str):
        device = self._get_device(name)

        if device.instance:
            return device.instance
        else:
            logger.info(f"Initialize device {name} from memory")
            instance = device.type(device.args)
            device.instance = instance
            return instance

    def _get_device(self, name):
        if name in self._drivers:
            logger.info(f"got device {name} from memory")
            return self._drivers[name]
        else:
            if self._backend:
                state = self._backend.get_latest_state(name)
                module = self._backend.get_driver_code(name)
                class_name = self._backend.get_type_name(name)
                if module and state:
                    instance = jsonpickle.loads(state)
                    device = _Lab_Device(name, None, None, instance)
                    scope = {"state": state}
                    split = class_name.split(".")
                    class_name = split.pop(len(split) - 1)
                    module_name = ".".join(split)
                    to_exec = (
                        f"{module}\n"
                        f"{class_name}.__module__='{module_name}'\n"
                        f"decoded = {class_name}.deserialize_function(state, {class_name})"
                    )
                    exec(to_exec, globals(), scope)
                    instance: Driver = scope["decoded"]
                    logger.info(f"got device {name} from db")
                    device = _LabTopologyDevice(name, None, None, instance)
                    self._drivers[name] = device
                    return device

        raise DriverNotFound()

    def add(self, name, type: Type[T], *args):
        if name in self._drivers:
            raise KeyError(f"instrument {name} already exist")
        if not issubclass(type, Driver):
            raise TypeError(f"instrument {name} is not an quantropy Driver")
        self._drivers[name] = _LabTopologyDevice(name, type, args[0])
        if self._backend:
            self._backend.save_driver(
                name,
                inspect.getsource(inspect.getmodule(type)),
                type.__module__ + "." + type.__name__,
            )

    def add_if_not_exist(self, name, type: Type[T], *args):
        if name not in self._drivers:
            code = self._backend.get_driver_code(name)
            if not code:
                self.add(name, type, args)

    def save_states(self):
        if self._backend:
            for key in self._drivers:
                device = self._drivers[key]
                str_snapshot = device.instance.snapshot(False)
                self._backend.save_state(device.name, str_snapshot)

    def all_drivers(self) -> Dict[str, Driver]:
        return {driver.name: driver.instance for driver in self._drivers.values()}


class ExperimentTopology:
    def __init__(self, topology) -> None:
        super().__init__()
        self._topology = topology

    def get(self, name: str):
        return self._topology.get(name)

    def lock(self):
        pass

    def release(self):
        pass

    def get_snapshot(self) -> Dict[str, str]:
        snapshots = {}
        all = self._topology.all_drivers()
        for driver in all:
            if all[driver]:
                snapshots[driver] = all[driver].snapshot(False)
        return snapshots

import abc
import inspect
from dataclasses import dataclass
from typing import TypeVar, Type, Optional, Dict, Any

import jsonpickle

T = TypeVar("T")


class LabTopologyBackend(abc.ABC):
    def __init__(self) -> None:
        super().__init__()

    @abc.abstractmethod
    def save_driver(self, name: str, driver: str):
        pass

    @abc.abstractmethod
    def save_state(self, name: str, state: str):
        pass

    @abc.abstractmethod
    def get_latest_state(self, name) -> str:
        pass


@dataclass
class _Lab_Device:
    name: str
    type: Optional[Type]
    args: Optional[Any]
    instance: Optional[object] = None


class DriverNotFound(BaseException):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class LabTopology:
    def __init__(self, backend: Optional[LabTopologyBackend] = None) -> None:
        super().__init__()
        self._drivers: Dict[str, _Lab_Device] = {}
        self._backend = backend

    def get(self, name: str):
        device = self._get_device(name)

        if device.instance:
            print(f"got device {name} from memory")
            return device.instance
        else:
            print(f"got device {name} from db")
            instance = device.type(device.args)
            device.instance = instance
            return instance

    def _get_device(self, name):
        if name in self._drivers:
            device = self._drivers[name]
        else:
            if self._backend:
                state = self._backend.get_latest_state(name)
                if state == "":
                    raise DriverNotFound()
                else:
                    instance = jsonpickle.loads(state)
                    device = _Lab_Device(name, None, None, instance)
                    self._drivers[name] = device
            else:
                raise DriverNotFound()
        return device

    def add(self, name, type: Type[T], *args):
        if name in self._drivers:
            raise KeyError(f"instrument {name} already exist")
        self._drivers[name] = _Lab_Device(name, type, args)
        if self._backend:
            self._backend.save_driver(name, inspect.getsource(inspect.getmodule(type)))

    def add_if_not_exist(self, name, type: Type[T], *args):
        if name not in self._drivers:
            self.add(name, type, args)

    # TODO Guy how to update driver

    def save_states(self):
        if self._backend:
            for key in self._drivers:
                device = self._drivers[key]
                self._backend.save_state(device.name, jsonpickle.dumps(device.instance))


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

    def get_snapshot(self):
        # todo should return a description of all instruments state
        pass

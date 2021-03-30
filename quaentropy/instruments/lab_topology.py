import abc
import inspect
from dataclasses import dataclass
from typing import TypeVar, Type, Optional, Dict, Any, Iterable

import jsonpickle

from quaentropy.api.errors import ResourceNotFound
from quaentropy.instruments.instrument_driver import Resource
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
    def get_all_states(self, name) -> Iterable[str]:
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
    is_entropy_resource: bool
    type: Optional[Type]
    args: Optional[Any]
    instance: Optional[Resource] = None


class LabTopology:
    def __init__(self, backend: Optional[LabTopologyBackend] = None) -> None:
        super().__init__()
        self._resources: Dict[str, _LabTopologyDevice] = {}
        self._backend = backend

    def get(self, name: str):
        device = self._get_device(name)

        return device.instance

    def _get_device(self, name):
        if name in self._resources:
            logger.debug(f"got device {name} from memory")
            return self._resources[name]
        else:
            if self._backend:
                state = self._backend.get_latest_state(name)
                module = self._backend.get_driver_code(name)
                class_name = self._backend.get_type_name(name)
                if module and state:
                    globals()["state"] = state
                    split = class_name.split(".")
                    class_name = split.pop(len(split) - 1)
                    module_name = ".".join(split)
                    to_exec = (
                        f"{module}\n"
                        f"{class_name}.__module__='{module_name}'\n"
                        f"decoded = {class_name}.deserialize_function(state, {class_name})"
                    )
                    exec(to_exec, globals(), globals())
                    instance: Resource = globals()["decoded"]
                    logger.debug(f"got device {name} from db")
                    device = _LabTopologyDevice(name, True, None, None, instance)
                    self._resources[name] = device
                    return device
                elif state:
                    instance = jsonpickle.loads(state)
                    return _LabTopologyDevice(name, False, None, None, instance)

        raise ResourceNotFound()

    def add_resource(self, name, type: Type[T], *args, save_source_to_db=True):
        if name in self._resources:
            raise KeyError(f"instrument {name} already exist")
        is_entropy_resource = issubclass(type, Resource)
        if not is_entropy_resource and save_source_to_db:
            raise TypeError(
                f"instrument {name} is not an quaentropy Resource and"
                f" source wont be saved to db"
            )
        logger.debug(f"Initialize device {name}")
        instance = type(*args)
        self._resources[name] = _LabTopologyDevice(
            name, is_entropy_resource, type, args, instance
        )
        if self._backend:
            if is_entropy_resource:
                self._backend.save_driver(
                    name,
                    inspect.getsource(inspect.getmodule(type)),
                    type.__module__ + "." + type.__name__,
                )
                state = instance.snapshot(False)
                self._backend.save_state(name, state)
            else:
                self._backend.save_driver(
                    name,
                    "",
                    type.__module__ + "." + type.__name__,
                )
                state = jsonpickle.dumps(instance)
                self._backend.save_state(name, state)

    def add_resource_if_not_exist(
        self,
        name,
        type: Type[T],
        *args,
        save_source_to_db=True,
    ):
        if name not in self._resources:
            code = self._backend.get_driver_code(name)
            state = self._backend.get_latest_state(name)
            if not code and not state:
                self.add_resource(
                    name, type, *args, save_source_to_db=save_source_to_db
                )

    def save_states(self):
        if self._backend:
            for key in self._resources:
                device = self._resources[key]
                if device.is_entropy_resource:
                    str_snapshot = device.instance.snapshot(False)
                else:
                    str_snapshot = jsonpickle.dumps(device.instance)
                self._backend.save_state(device.name, str_snapshot)

    def all_resources(self) -> Dict[str, Resource]:
        return {
            resource.name: resource.instance for resource in self._resources.values()
        }


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
        all = self._topology.all_resources()
        for resource in all:
            if all[resource] and isinstance(all[resource], Resource):
                snapshots[resource] = all[resource].snapshot(False)
        return snapshots

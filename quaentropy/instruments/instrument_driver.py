from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, List, Optional, Union, Type

import jsonpickle
import jsonpickle.ext.numpy as jsonpickle_numpy
import jsonpickle.ext.pandas as jsonpickle_pandas
from deepdiff import DeepDiff

jsonpickle_numpy.register_handlers()
jsonpickle_pandas.register_handlers()


class Driver(ABC):
    def __init__(self, name):
        super().__init__()
        self._name = name

    @abstractmethod
    def snapshot(self, update: bool) -> str:
        pass

    @staticmethod
    @abstractmethod
    def deserialize_function(snapshot: str, class_object: Type):
        pass

    def diff_from_snapshot(self, snapshot: str):
        copy = self.deserialize_function(snapshot, type(self))
        return DeepDiff(self, copy).to_dict()


class PickledDriver(Driver):
    def __init__(self, name: str):
        super().__init__(name)

    def snapshot(self, update: bool) -> str:
        frozen = jsonpickle.encode(self)
        decoded = jsonpickle.decode(frozen, classes=type(self))
        if len(DeepDiff(self, decoded).to_dict()):
            raise Exception("snapshot is not accurate")
        return frozen

    @staticmethod
    def deserialize_function(snapshot: str, class_object: Type):
        import jsonpickle

        decoded = jsonpickle.decode(snapshot, classes=class_object)
        return decoded


@dataclass
class Parameter:
    name: str
    unit: Optional[str]
    step: float
    scale: float
    offset: float
    vals: Any
    submodule: str = None


@dataclass
class Function:
    name: str
    parameters = None


class Instrument(PickledDriver):
    def __init__(self, name: str):
        super().__init__(name)
        self._parameters: List[Parameter] = []
        self._functions: List[Function] = []

    @abstractmethod
    def setup_driver(self):
        pass

    @abstractmethod
    def teardown_driver(self):
        pass

    def discover_driver_specs(self):
        pass

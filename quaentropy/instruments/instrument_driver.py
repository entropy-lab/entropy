from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional, Union

import jsonpickle
from deepdiff import DeepDiff


class Driver(ABC):
    def __init__(self, name):
        super().__init__()
        self._name = name

    @abstractmethod
    def snapshot(self, update: bool) -> str:
        pass

    @abstractmethod
    def load_from_snapshot(self, snapshot_or_path: Union[object, str]):
        pass

    @abstractmethod
    def diff_from_snapshot(self, snapshot_or_path: Union[object, str]):
        pass


import jsonpickle.ext.numpy as jsonpickle_numpy

jsonpickle_numpy.register_handlers()
import jsonpickle.ext.pandas as jsonpickle_pandas

jsonpickle_pandas.register_handlers()


class Virtual(Driver):
    def __init__(self, name: str):
        super().__init__(name)

    def snapshot(self, update: bool) -> str:
        frozen = jsonpickle.encode(self)
        decoded = jsonpickle.decode(self)
        if len(DeepDiff(self, decoded).to_dict()):
            raise Exception("snapshot is not accurate")
        return frozen

    def load_from_snapshot(self, snapshot: str):
        decoded = jsonpickle.decode(snapshot)
        self.__dict__ = decoded.__dict__  # todo guy????

    def diff_from_snapshot(self, snapshot: str):
        decoded = jsonpickle.decode(snapshot)
        return DeepDiff(self, decoded).to_dict()


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


class Instrument(Driver):
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

    @abstractmethod
    def snapshot(self, update: bool):
        pass

    def load_from_snapshot(self, snapshot_or_path: Union[object, str]):
        raise NotImplementedError()

    def diff_from_snapshot(self, snapshot_or_path: Union[object, str]):
        raise NotImplementedError()

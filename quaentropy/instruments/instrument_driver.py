from abc import ABC, abstractmethod
from dataclasses import dataclass
from difflib import ndiff
from typing import Any, List, Optional

import jsonpickle
import jsonpickle.ext.numpy as jsonpickle_numpy
import jsonpickle.ext.pandas as jsonpickle_pandas
from deepdiff import DeepDiff

jsonpickle_numpy.register_handlers()
jsonpickle_pandas.register_handlers()


class Resource(ABC):
    def __init__(self, name):
        super().__init__()
        self._name = name

    @abstractmethod
    def snapshot(self, update: bool) -> str:
        pass

    def revert_to_snapshot(self, snapshot: str):
        raise NotImplementedError(
            f"resource {self._name} has not implemented revert to snapshot"
        )

    def diff_from_snapshot(self, other_snapshot: str):
        snapshot = self.snapshot(False)
        return ndiff(snapshot, other_snapshot)


class PickledResource(Resource):
    def __init__(self, name: str):
        super().__init__(name)

    def snapshot(self, update: bool) -> str:
        frozen = jsonpickle.encode(self)
        decoded = jsonpickle.decode(frozen, classes=type(self))
        if len(DeepDiff(self, decoded).to_dict()):
            raise Exception("snapshot is not accurate")
        return frozen

    def revert_to_snapshot(self, snapshot: str):
        super().revert_to_snapshot(snapshot)


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


class Instrument(PickledResource):
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

    def __del__(self):
        # todo just if opened?
        self.teardown_driver()

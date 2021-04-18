from abc import ABC, abstractmethod
from dataclasses import dataclass
from difflib import ndiff
from typing import Any, List, Optional

import jsonpickle
import jsonpickle.ext.numpy as jsonpickle_numpy
import jsonpickle.ext.pandas as jsonpickle_pandas

jsonpickle_numpy.register_handlers()
jsonpickle_pandas.register_handlers()

Entropy_Resource_Name = "entropy_name"


class Resource(ABC):
    def __init__(self, **kwargs):
        super().__init__()
        self._entropy_name = kwargs.get(Entropy_Resource_Name, "")

    @abstractmethod
    def snapshot(self, update: bool) -> str:
        pass

    def revert_to_snapshot(self, snapshot: str):
        raise NotImplementedError(
            f"resource {self.__class__.__qualname__} has not implemented revert to snapshot"
        )

    def diff_from_snapshot(self, other_snapshot: str):
        snapshot = self.snapshot(False)
        return ndiff(snapshot, other_snapshot)

    def set_entropy_name(self, name: str):
        self._entropy_name = name


class PickledResource(Resource):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def snapshot(self, update: bool) -> str:
        return jsonpickle.encode(self)


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
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
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
        self.teardown_driver()

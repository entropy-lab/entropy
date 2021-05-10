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


@dataclass
class Parameter:
    """"""

    name: str
    unit: Optional[str]
    step: float
    scale: float
    offset: float
    vals: Any
    submodule: str = None


@dataclass
class Function:
    """"""

    name: str
    parameters = None


@dataclass
class DriverSpec:
    """"""

    parameters: List[Parameter]
    functions: List[Function]
    undeclared_functions: List[Function]


class Resource(ABC):
    """
    An abstract class with extra functionality.
    Entropy will be able to use the snapshot and revert_to_snapshot in experiments,
    making it easier to control the resource, and save metadata
    The he resource might be  an actual instrument in the lab as well.
    Entropy will call the "connect" when starting the driver,
    and the "teardown" on close, so no connections will be left open
    """

    def __init__(self, **kwargs):
        super().__init__()
        self._entropy_name = kwargs.get(Entropy_Resource_Name, "")

    @abstractmethod
    def connect(self):
        """
        Start the driver, open needed connections or call any other functions that
        are needed for using the driver
        """
        pass

    @abstractmethod
    def teardown(self):
        """
        Gracefully close all driver connections and links
        """
        pass

    def get_dynamic_driver_specs(self) -> DriverSpec:
        """
        add metadata for the driver functionality, if discovered dynamically
        """
        pass

    def __del__(self):
        self.teardown()

    def get_instance(self):
        """
            Returns the actual instance of the driver.

            If the driver is wrapping a different instrument, should return the
            actual instance.

        :return:
        """
        return self

    @abstractmethod
    def snapshot(self, update: bool) -> str:
        """
            serialize the current resource state to string
            The completeness of the state is up to the implementation.
            Best practice will be to create the most accurate representation,
            allowing "revert_to_snapshot" function to revert the resource to the
            same exact state
        :param update: Whether to update the resource state actively, or use the
                        cached data
        """
        pass

    def revert_to_snapshot(self, snapshot: str):
        """
            revert the current resource to a different state, using a saved
            snapshot
        :param snapshot: a serialized state of the resource, created by
                        the "snapshot" function
        """
        raise NotImplementedError(
            f"resource {self.__class__.__qualname__} has not implemented revert to snapshot"
        )

    def diff_from_snapshot(self, other_snapshot: str):
        """
            calculates the difference between the given snapshot and current resource state
        :param other_snapshot: a snapshot of this resource
        """
        snapshot = self.snapshot(False)
        return ndiff(snapshot, other_snapshot)

    def set_entropy_name(self, name: str):
        """
        helper function for entropy, to set names of resources
        """
        self._entropy_name = name


class PickledResource(Resource):
    """
    An implemetation of "snapshot" function using jsonpickle
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def snapshot(self, update: bool = True) -> str:
        """
            serialize the current resource state to string with jsonpickle
             package.
        :param update: Whether to update the resource state actively, or use the
                        cached data
        """
        return jsonpickle.encode(self)

    @abstractmethod
    def connect(self):
        """
        Start the driver, open needed connections or call any other functions that
        are needed for using the driver
        """
        pass

    @abstractmethod
    def teardown(self):
        """
        Gracefully close all driver connections and links
        """
        pass

    def revert_to_snapshot(self, snapshot: str):
        """
            revert the current resource to a different state, using a saved
            snapshot
        :param snapshot: a serialized state of the resource, created by
                        the "snapshot" function
        """
        raise NotImplementedError(
            f"resource {self.__class__.__qualname__} has not implemented revert to snapshot"
        )

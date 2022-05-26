from typing import Type, Union, Optional, List

import jsonpickle
from qcodes import Function, ChannelList
from qcodes.instrument.base import InstrumentBase
from qcodes.instrument.parameter import _BaseParameter

from entropylab.pipeline.api.errors import EntropyError
from entropylab.components.instrument_driver import (
    Parameter,
    Function as EntropyFunction,
    Entropy_Resource_Name,
    DriverSpec,
    Resource,
)
from entropylab.logger import logger


def _transform_parameter(parameter: _BaseParameter) -> Parameter:
    if hasattr(parameter, "unit"):
        unit = parameter.unit
    else:
        unit = None
    return Parameter(
        parameter.name,
        unit,
        parameter.step,
        parameter.scale,
        parameter.offset,
        parameter.vals,
        "",
    )


def _transform_function(function: Function) -> EntropyFunction:
    return EntropyFunction(function.name)


def _get_transformed_parameters(parameters):
    return [_transform_parameter(p) for p in parameters]


def _get_transformed_functions(functions):
    return [_transform_function(f) for f in functions]


def _get_undeclared_functions(instance, functions: List[EntropyFunction]):
    existing_functions_names = [func.name for func in functions]
    instrument_base_functions = [
        func
        for func in dir(InstrumentBase)
        if callable(getattr(InstrumentBase, func)) and not func.startswith("__")
    ]
    method_list = [
        func
        for func in dir(instance)
        if callable(getattr(instance, func))
        and not func.startswith("__")
        and func not in instrument_base_functions
        and not isinstance(getattr(instance, func), _BaseParameter)
        and func not in existing_functions_names
    ]
    entropy_extra_methods = [EntropyFunction(func) for func in method_list]
    return entropy_extra_methods


class QcodesAdapter(Resource):
    """"""

    def __init__(self, driver: Type[InstrumentBase], *args, **kwargs):
        super().__init__(**kwargs)
        self._instance: InstrumentBase = None
        self._driver = driver
        self._args = args
        self._kwargs = kwargs
        self._kwargs.pop(Entropy_Resource_Name, None)
        self._is_connected = False

    def get_dynamic_driver_specs(self) -> DriverSpec:
        """
        qcodes drivers only get a complete list of parameters and actions when loading
        and connecting to device
        this function will add all the parameters to the driver, can be used outside and saved

        """
        self.connect()
        parameters = _get_transformed_parameters(self._instance.parameters.values())
        functions = _get_transformed_functions(self._instance.functions.values())
        undeclared_functions = _get_undeclared_functions(self._instance, functions)
        for name, sub in self._instance.submodules:
            sub_params, sub_functions = self._extract_submodule_specs(name, sub)
            parameters.extend(sub_params)
            functions.extend(sub_functions)

        self.teardown()
        return DriverSpec(parameters, functions, undeclared_functions)

    def _extract_submodule_specs(
        self, submodule_name, submodule: Union["InstrumentBase", "ChannelList"]
    ):
        parameters = _get_transformed_parameters(submodule.parameters)
        functions = _get_transformed_functions(submodule.functions)

        for p in parameters:
            p.submodule = f"{submodule_name}"
        for f in functions:
            f.submodule = f"{submodule_name}"

        for name, sub in self._instance.submodules:
            self._extract_submodule_specs(f"{submodule_name}_{name}", sub)

        return parameters, functions

    def connect(self):
        self._is_connected = True
        self._instance = self._driver(*self._args, **self._kwargs)

    def get_instance(self):
        if self._is_connected:
            return self._instance
        else:
            self.connect()
            return self._instance

    def teardown(self):
        try:
            if self._is_connected:
                del self._instance
            self._is_connected = False
        except Exception as e:
            logger.debug("Exception during resource teardown", e)

    @property
    def instance(self):
        return self._instance

    def snapshot(self, update: Optional[bool] = True) -> str:
        """
        Use qcodes snapshot method to save the state of the instrument.
        the qcodes snapshot dictionary is then serialized using jsonpickle,
        or using the str() function if dictionary can not be pickled.
        """
        if self._is_connected:
            qcodes_snapshot_dict = self._instance.snapshot(update=update)
            try:
                serialized_snapshot = jsonpickle.dumps(qcodes_snapshot_dict)
            except Exception:
                serialized_snapshot = str(qcodes_snapshot_dict)
            return serialized_snapshot
        else:
            raise EntropyError("can not save snapshot of disconnected instrument")

    def revert_to_snapshot(self, snapshot: str):
        raise NotImplementedError(
            f"resource {self.__class__.__qualname__} has not implemented revert to snapshot"
        )

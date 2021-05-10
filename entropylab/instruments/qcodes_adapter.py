from typing import Type, Union, Optional, List

from qcodes import Function, ChannelList
from qcodes.instrument.base import InstrumentBase
from qcodes.instrument.parameter import _BaseParameter

from entropylab.instruments.instrument_driver import (
    Instrument as EntropyInstrument,
    Parameter,
    Function as EntropyFunction,
    Entropy_Resource_Name,
    DriverSpec,
)


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


class QcodesAdapter(EntropyInstrument):
    """"""

    def __init__(self, driver: Type[InstrumentBase], *args, **kwargs):
        super().__init__(**kwargs)
        self._instance: InstrumentBase = None
        self._driver = driver
        self._args = args
        self._kwargs = kwargs
        self._kwargs.pop(Entropy_Resource_Name, None)

    def get_dynamic_driver_specs(self) -> DriverSpec:
        """
        qcodes drivers only get a complete list of parameters and actions when loading
        and connecting to device
        this function will add all the parameters to the driver, can be used outside and saved

        """
        self.setup_driver()
        parameters = _get_transformed_parameters(self._instance.parameters.values())
        functions = _get_transformed_functions(self._instance.functions.values())
        undeclared_functions = _get_undeclared_functions(self._instance, functions)
        for name, sub in self._instance.submodules:
            sub_params, sub_functions = self._extract_submodule_specs(name, sub)
            parameters.extend(sub_params)
            functions.extend(sub_functions)

        self.teardown_driver()
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

    def setup_driver(self):
        self._instance = self._driver(*self._args, **self._kwargs)

    def teardown_driver(self):
        try:
            del self._instance
        except Exception as e:
            print(e)

    @property
    def instance(self):
        return self._instance

    def snapshot(self, update: Optional[bool]):
        qcodes_snapshot_dict = self._instance.snapshot(update=update)
        return qcodes_snapshot_dict

    def revert_to_snapshot(self, snapshot: str):
        raise NotImplementedError(
            f"resource {self.__class__.__qualname__} has not implemented revert to snapshot"
        )

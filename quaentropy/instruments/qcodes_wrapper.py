import types
from typing import Type, Union, Optional

from qcodes import Function, ChannelList
from qcodes.instrument.base import InstrumentBase
from qcodes.instrument.parameter import _BaseParameter

from quaentropy.instruments.instrument_driver import (
    Instrument as EntropyInstrument,
    Parameter,
    Function as EntropyFunction,
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


class QcodesWrapper(EntropyInstrument):
    def __init__(self, driver: Type[InstrumentBase], name: str, *args):
        super().__init__(name)
        self._instance: InstrumentBase = None
        self._driver = driver
        self._args = args

    def discover_driver_specs(self):
        """
        qcodes drivers only get a complete list of parameters and actions when loading
        and connecting to device
        this function will add all the parameters to the driver, can be used outside and saved
        """
        self.setup_driver()
        self._parameters.extend(
            _get_transformed_parameters(self._instance.parameters.values())
        )
        self._functions.extend(
            _get_transformed_functions(self._instance.functions.values())
        )
        for name, sub in self._instance.submodules:
            self._extract_submodule_specs(name, sub)

        self._register_parameters_and_functions()

        self.teardown_driver()

    def _register_parameters_and_functions(self):
        for param in self._parameters:
            setattr(
                self,
                f"set_{param.name}",
                types.MethodType(
                    lambda self, value: self._instance.set(param.name, value), self
                ),
            )
            setattr(
                self,
                f"get_{param.name}",
                types.MethodType(
                    lambda self, value: self._instance.get(param.name, value), self
                ),
            )
        for func in self._functions:
            setattr(
                self,
                f"call_{func.name}",
                types.MethodType(
                    lambda obj, *args: obj._instance.call(func.name, args), self
                ),
            )

    def _extract_submodule_specs(
        self, submodule_name, submodule: Union["InstrumentBase", "ChannelList"]
    ):
        parameters = _get_transformed_parameters(submodule.parameters)
        functions = _get_transformed_functions(submodule.functions)

        for p in parameters:
            p.submodule = f"{submodule_name}"
        for f in functions:
            f.submodule = f"{submodule_name}"

        self._parameters.extend(parameters)
        self._functions.extend(functions)
        for name, sub in self._instance.submodules:
            self._extract_submodule_specs(f"{submodule_name}_{name}", sub)

    def setup_driver(self):
        self._instance = self._driver(*self._args)

    def teardown_driver(self):
        del self._instance

    def snapshot(self, update: Optional[bool]):
        return self._instance.snapshot(update=update)

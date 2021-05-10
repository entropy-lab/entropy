from typing import Optional, Dict, Any

import pytest


@pytest.mark.skip()
def test_qcodes_dummy():
    from qcodes.instrument.base import InstrumentBase as qcodes_InstrumentBase
    from entropylab.instruments.qcodes_adapter import QcodesAdapter

    class MockQcodesDriver(qcodes_InstrumentBase):
        def __init__(
            self, name: str, metadata: Optional[Dict[Any, Any]] = None
        ) -> None:
            super().__init__(name, metadata)
            self.add_parameter("p")
            setter = lambda val: print(val)
            getter = lambda: 1
            self.add_parameter("s", set_cmd=self.setter, get_cmd=self.getter)
            self.add_parameter("g", set_cmd=setter, get_cmd=getter)

        def setter(self, val):
            print(val)
            self.s = val

        def getter(self):
            return self.s

        def free_function(self):
            print("i'm free")

    class QcodesDummy(QcodesAdapter):
        def __init__(self):
            super().__init__(MockQcodesDriver, "QcodesDummy")

        def revert_to_snapshot(self, snapshot: str):
            pass

    dummy = QcodesDummy()
    print(dummy)
    dummy.setup_driver()
    dummy.instance.set("s", "printed")
    dummy.instance.free_function()
    dummy.instance.set("g", "g")
    assert dummy.instance.get("s") == "printed"
    assert dummy.instance.get("g") == 1
    dummy.teardown_driver()


@pytest.mark.skip()
def test_qcodes_dummy_object():
    # Importing in test so general pytest discovery wont enforce qcodes installation
    from qcodes.instrument.base import InstrumentBase as qcodes_InstrumentBase
    from entropylab.instruments.qcodes_adapter import QcodesAdapter

    class MockQcodesDriver(qcodes_InstrumentBase):
        def __init__(
            self, name: str, metadata: Optional[Dict[Any, Any]] = None
        ) -> None:
            super().__init__(name, metadata)
            self.add_parameter("p")
            setter = lambda val: print(val)
            getter = lambda: 1
            self.add_parameter("s", set_cmd=self.setter, get_cmd=self.getter)
            self.add_parameter("g", set_cmd=setter, get_cmd=getter)

        def setter(self, val):
            print(val)
            self.s = val

        def getter(self):
            return self.s

        def free_function(self):
            print("i'm free")

    dummy = QcodesAdapter(MockQcodesDriver, "dummy_inst")
    dummy.setup_driver()
    dummy.instance.set("s", "printed")
    dummy.instance.free_function()
    dummy.instance.set("g", "g")
    assert dummy.instance.get("s") == "printed"
    assert dummy.instance.get("g") == 1
    dummy.teardown_driver()


@pytest.mark.skip()
def test_qcodes_dummy_object_dynamic_spec():
    # Importing in test so general pytest discovery wont enforce qcodes installation
    from qcodes.instrument.base import InstrumentBase as qcodes_InstrumentBase
    from entropylab.instruments.qcodes_adapter import QcodesAdapter

    class MockQcodesDriver(qcodes_InstrumentBase):
        def __init__(
            self, name: str, metadata: Optional[Dict[Any, Any]] = None
        ) -> None:
            super().__init__(name, metadata)
            self.add_parameter("p")
            setter = lambda val: print(val)
            getter = lambda: 1
            self.add_parameter("s", set_cmd=self.setter, get_cmd=self.getter)
            self.add_parameter("g", set_cmd=setter, get_cmd=getter)

        def setter(self, val):
            print(val)
            self.s = val

        def getter(self):
            return self.s

        def free_function(self):
            print("i'm free")

    dummy = QcodesAdapter(MockQcodesDriver, "dummy_inst")
    driver_spec = dummy.get_dynamic_driver_specs()
    print(driver_spec)
    assert len(driver_spec.parameters) == 3
    assert driver_spec.parameters[0].name == "p"
    assert driver_spec.parameters[1].name == "s"
    assert driver_spec.parameters[2].name == "g"
    assert len(driver_spec.functions) == 0
    assert len(driver_spec.undeclared_functions) == 3
    assert driver_spec.undeclared_functions[0].name == "free_function"

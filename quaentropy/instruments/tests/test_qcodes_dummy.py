from typing import Optional, Dict, Any

import pytest


@pytest.mark.skip()
def test_qcodes_dummy():
    from qcodes.instrument.base import InstrumentBase as qcodes_InstrumentBase
    from quaentropy.instruments.qcodes_wrapper import QcodesWrapper

    class MockQcodesDriver(qcodes_InstrumentBase):
        def __init__(
            self, name: str, metadata: Optional[Dict[Any, Any]] = None
        ) -> None:
            super().__init__(name, metadata)
            self.add_parameter("p")
            setter = lambda val: print(val)
            self.add_parameter("s", set_cmd=self.setter, get_cmd=self.getter)

        def setter(self, val):
            print(val)
            self.s = val

        def getter(self):
            return self.s

    class QcodesDummy(QcodesWrapper):
        def __init__(self):
            super().__init__(MockQcodesDriver, "QcodesDummy")

    dummy = QcodesDummy()
    dummy.discover_driver_specs()
    print(dummy)
    dummy.setup_driver()
    dummy.set_s("printed")
    assert dummy.get_s() == "printed"
    dummy.teardown_driver()


@pytest.mark.skip()
def test_qcodes_dummy_object():
    from qcodes.instrument.base import InstrumentBase as qcodes_InstrumentBase
    from quaentropy.instruments.qcodes_wrapper import QcodesWrapper

    class MockQcodesDriver(qcodes_InstrumentBase):
        def __init__(
            self, name: str, metadata: Optional[Dict[Any, Any]] = None
        ) -> None:
            super().__init__(name, metadata)
            self.add_parameter("p")
            setter = lambda val: print(val)
            self.add_parameter("s", set_cmd=self.setter, get_cmd=self.getter)

        def setter(self, val):
            print(val)
            self.s = val

        def getter(self):
            return self.s

    dummy = QcodesWrapper(MockQcodesDriver, "dummy_inst")
    dummy.discover_driver_specs()
    print(dummy)

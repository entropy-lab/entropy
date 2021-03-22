from typing import Optional, Dict, Any

from qcodes.instrument.base import InstrumentBase as qcodes_InstrumentBase

from quaentropy.instruments.qcodes_wrapper import QcodesWrapper


class MockQcodesDriver(qcodes_InstrumentBase):
    def __init__(self, name: str, metadata: Optional[Dict[Any, Any]] = None) -> None:
        super().__init__(name, metadata)
        self.add_parameter("p")
        setter = lambda val: print(val)
        self.add_parameter("s", set_cmd=setter)


class QcodesDummy(QcodesWrapper):
    def __init__(self, name: str):
        super().__init__(MockQcodesDriver, name)


def test_qcodes_dummy():
    dummy = QcodesDummy("dummy_inst")
    dummy.discover_driver_specs()
    print(dummy)
    dummy.setup_driver()
    dummy.set_s("printed")
    dummy.teardown_driver()


def test_qcodes_dummy_object():
    dummy = QcodesWrapper(MockQcodesDriver, "dummy_inst")
    dummy.discover_driver_specs()
    print(dummy)

############ This file is commented out so qcodes installation will not be enforced #############
from typing import Optional, Dict, Any

from qcodes.instrument.base import InstrumentBase as qcodes_InstrumentBase

from entropylab import SqlAlchemyDB, LabResources
from entropylab.instruments.qcodes_adapter import QcodesAdapter


class MockQcodesDriver(qcodes_InstrumentBase):
    def __init__(self, name: str, metadata: Optional[Dict[Any, Any]] = None) -> None:
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


def test_adding_qcodes_inst_to_lab():
    db = SqlAlchemyDB()
    lab = LabResources(db)
    lab.register_resource("dummy_inst", QcodesDummy)
    lab.register_resource(
        "dummy_inst1",
        QcodesAdapter,
        [MockQcodesDriver, "dummy_inst1"],
        dynamic_driver_specs_discovery=True,
    )

    lab1 = LabResources(db)
    dummy: QcodesAdapter = lab1.get_resource("dummy_inst")
    dummy.connect()
    dummy.instance.set("s", "printed")
    assert dummy.instance.get("s") == "printed"

    dummy: QcodesAdapter = lab1.get_resource("dummy_inst")
    dummy.get_dynamic_driver_specs()
    dummy.connect()
    dummy.instance.set("s", "printed")
    assert dummy.instance.get("s") == "printed"

    dummy: QcodesAdapter = lab1.get_resource("dummy_inst1")
    dummy.connect()
    dummy.instance.set("s", "printed")
    assert dummy.instance.get("s") == "printed"

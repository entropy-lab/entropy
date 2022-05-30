import jsonpickle
import pytest

from entropylab.pipeline.api.errors import ResourceNotFound
from entropylab.components.instrument_driver import PickledResource
from entropylab.components.lab_topology import LabResources, ExperimentResources
from entropylab.pipeline.results_backend.sqlalchemy.db import SqlAlchemyDB


def test_has_resource():
    db = SqlAlchemyDB()
    lab = LabResources(db)
    lab.register_resource("integer", int, [1])
    print(",".join(lab.all_resources()))

    exp = ExperimentResources(db)
    exp.add_temp_resource("float", 0.2)
    exp.import_lab_resource("integer")

    assert exp.has_resource("integer") == True
    assert exp.has_resource("float") == True
    assert exp.has_resource("does not exist") == False


def test_get_resource():
    db = SqlAlchemyDB()
    lab = LabResources(db)
    lab.register_resource("integer", int, [1])

    exp = ExperimentResources(db)
    exp.add_temp_resource("float", 0.2)
    exp.import_lab_resource("integer")

    assert exp.get_resource("integer") == 1
    assert exp.get_resource("float") == 0.2

    with pytest.raises(KeyError):
        exp.get_resource("does not exist")


def test_import_twice():
    db = SqlAlchemyDB()
    lab = LabResources(db)
    lab.register_resource("integer", int, [1])

    with pytest.raises(Exception):
        exp = ExperimentResources(db)
        exp.add_temp_resource("float", 0.2)
        exp.import_lab_resource("float")

    with pytest.raises(Exception):
        exp = ExperimentResources(db)
        exp.import_lab_resource("integer")
        exp.import_lab_resource("integer")

    with pytest.raises(Exception):
        exp = ExperimentResources(db)
        exp.add_temp_resource("float", 0.2)
        exp.add_temp_resource("float", 0.2)


def test_remove_resource():
    db = SqlAlchemyDB()
    lab = LabResources(db)
    lab.register_resource("integer", int, [1])
    lab.remove_resource("integer")
    with pytest.raises(ResourceNotFound):
        lab.get_resource("integer")


class Dummy(PickledResource):
    def __init__(self, **kwargs):
        self.a = 1
        super().__init__(**kwargs)

    def connect(self):
        pass

    def teardown(self):
        pass

    def revert_to_snapshot(self, snapshot: str):
        self.a = jsonpickle.loads(snapshot).a

    def do_something(self):
        self.a = 2


def test_get_snapshot():
    db = SqlAlchemyDB()
    lab = LabResources(db)
    lab.register_resource("dummy", Dummy)
    dummy = lab.get_resource("dummy")
    dummy.do_something()
    lab.save_snapshot("dummy", "first")
    print(lab.get_snapshot("dummy", "first"))

    exp = ExperimentResources(db)
    exp.import_lab_resource("dummy", snapshot_name="first")
    exp.start_experiment()
    dummy = exp.get_resource("dummy")
    assert dummy.a == 2

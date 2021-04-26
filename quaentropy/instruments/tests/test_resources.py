import pytest

from quaentropy.instruments.lab_topology import LabResources, ExperimentResources
from quaentropy.results_backend.sqlalchemy.db import SqlAlchemyDB


def test_has_resource():
    db = SqlAlchemyDB()
    lab = LabResources(db)
    lab.register_resource("integer", int, [1])
    print(",".join(lab.list_resources()))

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

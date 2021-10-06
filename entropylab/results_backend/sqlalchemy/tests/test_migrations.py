import pytest

from entropylab import SqlAlchemyDB
from entropylab.results_backend.sqlalchemy.tests.test_utils import (
    create_test_project,
    delete_if_exists,
)


@pytest.mark.parametrize("path", [None, ":memory:"])
def test_ctor_creates_up_to_date_schema_when_in_memory(path: str):
    try:
        # act
        target = SqlAlchemyDB(path=path, echo=True)
        # assert
        cur = target._engine.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'Results'"
        )
        res = cur.fetchone()
        cur.close()
        assert "saved_in_hdf5" in res[0]
    finally:
        delete_if_exists("./entropy.hdf5")


@pytest.mark.parametrize(
    "db_template, expected_to_raise",
    [
        (None, False),  # new db
        ("./db_templates/empty.db", False),  # existing but empty
        ("./db_templates/initial.db", True),  # revision 1318a586f31d
        ("./db_templates/with_saved_in_hdf5_col.db", False),  # revision 04ae19b32c08
    ],
)
def test_ctor_ensures_latest_migration(
    db_template: str, expected_to_raise: bool, request
):
    # arrange
    if db_template is not None:
        test_project_dir = create_test_project(request, db_template)
    else:
        test_project_dir = create_test_project(request)
    try:
        if expected_to_raise:
            with pytest.raises(RuntimeError):
                # act & assert
                SqlAlchemyDB(path=test_project_dir, echo=True)
        else:
            SqlAlchemyDB(path=test_project_dir, echo=True)
    finally:
        # clean up
        delete_if_exists(test_project_dir)

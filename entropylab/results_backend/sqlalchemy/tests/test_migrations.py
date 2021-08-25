import os
from datetime import datetime
from pathlib import Path
from shutil import copyfile

import pytest

from entropylab import SqlAlchemyDB


@pytest.fixture(scope="function")
def change_test_dir(request):
    os.chdir(request.fspath.dirname)
    yield
    os.chdir(request.config.invocation_dir)


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
        _delete_if_exists("./entropy.hdf5")


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
        db_under_test = _get_test_file_name(db_template)
        _copy_db(db_template, db_under_test, request)
    else:
        db_under_test = _get_test_file_name("tests_cache/new.db")
    hdf5_under_test = Path(db_under_test).with_suffix(".hdf5")
    try:
        if expected_to_raise:
            with pytest.raises(Exception):
                # act & assert
                SqlAlchemyDB(path=db_under_test, echo=True)
        else:
            SqlAlchemyDB(path=db_under_test, echo=True)
    finally:
        # clean up
        _delete_if_exists(db_under_test)
        _delete_if_exists(hdf5_under_test)


def _get_test_file_name(filename):
    timestamp = f"{datetime.now():%Y-%m-%d-%H-%M-%S}"
    return filename.replace("db_templates", "tests_cache").replace(
        ".db", f"_{timestamp}.db"
    )


def _copy_db(src, dst, request):
    """ Copy the source DB (path relative to test file) to the destination dir """
    copyfile(os.path.join(request.fspath.dirname, src), dst)


def _delete_if_exists(filename: str):
    if os.path.isfile(filename):
        os.remove(filename)

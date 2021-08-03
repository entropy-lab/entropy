import os
import sqlite3
from datetime import datetime
from shutil import copyfile

import pytest

from entropylab import SqlAlchemyDB


@pytest.mark.parametrize(
    "db", [
        None,  # new db
        "empty.db",
        "initial.db",  # revision 1318a586f31d
        "with_saved_in_hdf5_col.db"  # revision 04ae19b32c08
    ])
def test_migrations(db: str):
    # arrange
    if db is not None:
        db_under_test = get_test_file_name(db)
        copyfile(os.path.join("./db_templates/", db), db_under_test)
    else:
        db_under_test = get_test_file_name("new.db")
    try:
        # act
        SqlAlchemyDB(path=db_under_test, echo=True)
        # assert
        conn = sqlite3.connect(db_under_test)
        cur = conn.execute("SELECT sql FROM sqlite_master WHERE name = 'Results'")
        res = cur.fetchone()
        conn.close()
        assert "saved_in_hdf5" in res[0]
    finally:
        # clean up
        os.remove(db_under_test)

# Add a test for db with initial schema and values in tables.
# put test dbs in tests_cache folder (so gitignored)
# test revision matrix


def get_test_file_name(filename):
    timestamp = f"{datetime.now():%Y-%m-%d-%H-%M-%S}"
    return filename.replace(".db", f"_{timestamp}.db")

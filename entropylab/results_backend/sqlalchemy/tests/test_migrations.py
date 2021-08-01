import os
import sqlite3
from datetime import datetime
from shutil import copyfile

import pytest

from entropylab import SqlAlchemyDB


@pytest.mark.parametrize(
    "db", [
        "./empty.db",
        "./initial.db",
        "./with_saved_in_hdf5_col.db"])
def test_migrations(db: str):
    # arrange
    db_under_test = get_test_file_name(db)
    copyfile(db, db_under_test)
    try:
        # act
        SqlAlchemyDB(path=db_under_test, echo=True)
        # assert
        conn = sqlite3.connect(db_under_test)
        cur = conn.execute("SELECT sql FROM sqlite_master WHERE name = 'Results'")
        sql = cur.fetchone()
        assert "saved_in_hdf5" in sql
    finally:
        os.remove(db_under_test)
        pass


def get_test_file_name(filename):
    timestamp = f"{datetime.now():%Y-%m-%d-%H-%M-%S}"
    return filename.replace(".db", f"_{timestamp}.db")

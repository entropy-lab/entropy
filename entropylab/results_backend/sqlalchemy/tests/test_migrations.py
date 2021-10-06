import pytest

from entropylab import SqlAlchemyDB


@pytest.mark.parametrize("path", [None, ":memory:"])
def test_ctor_creates_up_to_date_schema_when_in_memory(path: str):
    # act
    target = SqlAlchemyDB(path=path, echo=True)
    # assert
    cur = target._engine.execute("SELECT sql FROM sqlite_master WHERE name = 'Results'")
    res = cur.fetchone()
    cur.close()
    assert "saved_in_hdf5" in res[0]


@pytest.mark.parametrize(
    "initialized_project_dir_path",
    [
        None,  # new db
        "./db_templates/empty.db",  # existing but empty
        "./db_templates/with_saved_in_hdf5_col.db",  # latest revision: 04ae19b32c08
    ],
    indirect=True,
)
def test_ctor_ensures_latest_migration(
    initialized_project_dir_path,
):
    SqlAlchemyDB(initialized_project_dir_path)


@pytest.mark.parametrize(
    "initialized_project_dir_path",
    ["./db_templates/initial.db"],  # revision 1318a586f31d
    indirect=True,
)
def test_ctor_throws_when_db_is_not_up_to_date(
    initialized_project_dir_path,
):
    with pytest.raises(RuntimeError):
        SqlAlchemyDB(initialized_project_dir_path)

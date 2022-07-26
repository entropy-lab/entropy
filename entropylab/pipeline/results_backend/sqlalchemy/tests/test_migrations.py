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
        "empty.db",  # existing but empty
        "empty_after_2022-07-03-08-56-23_da8d38e19ff8_matplotlib_figures.db",
        # ⬆ latest version in pipeline/results_backend/sqlalchemy/alembic/versions
    ],
    indirect=True,
)
def test_ctor_ensures_latest_migration(
    initialized_project_dir_path,
):
    SqlAlchemyDB(initialized_project_dir_path)


@pytest.mark.parametrize(
    "initialized_project_dir_path",
    ["empty_after_2021-08-01-13-45-43_1318a586f31d_initial_migration.db"],
    indirect=True,
)
def test_ctor_throws_when_db_is_not_up_to_date(
    initialized_project_dir_path,
):
    with pytest.raises(RuntimeError):
        SqlAlchemyDB(initialized_project_dir_path)

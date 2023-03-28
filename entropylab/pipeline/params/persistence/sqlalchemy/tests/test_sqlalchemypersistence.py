from uuid import UUID

import pandas as pd
import pytest
from sqlalchemy import text

from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.params.param_store import Param
from entropylab.pipeline.params.persistence.persistence import Commit
from entropylab.pipeline.params.persistence.sqlalchemy.sqlalchemypersistence import (
    SqlAlchemyPersistence,
)


@pytest.fixture()
def target(tmp_path) -> SqlAlchemyPersistence:
    file_path = tmp_path / "sqlite.db"
    url = f"sqlite:///{file_path}"
    return SqlAlchemyPersistence(url)


""" constructor """


def test_ctor_creates_schema(target):
    with target.engine.connect() as connection:
        cursor = connection.execute(
            text("SELECT sql FROM sqlite_master WHERE type = 'table'")
        )
        assert len(cursor.fetchall()) == 3


def test_ctor_stamps_head(target):
    with target.engine.connect() as connection:
        cursor = connection.execute(text("SELECT version_num FROM alembic_version"))
        assert cursor.first() == ("000c6a88457f",)


""" get_commit """


def test_get_commit_when_commit_id_exists_then_commit_is_returned(target):
    commit_id = "f74c808e-2388-4b0a-a051-17eb9eb14339"
    with target.engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO 'commit' VALUES "
                f"('{commit_id}', '{pd.Timestamp.now()}', 'bar', '0', '0');"
            )
        )
    actual = target.get_commit(commit_id)
    assert actual.id == UUID(commit_id)


def test_get_commit_when_commit_id_does_not_exist_then_error_is_raised(target):
    with pytest.raises(EntropyError):
        target.get_commit("foo")


def test_get_commit_when_commit_num_exists_then_commit_is_returned(target):
    commit_id1 = "f74c808e-2388-4b0a-a051-17eb9eb11111"
    commit_id2 = "f74c808e-2388-4b0a-a051-17eb9eb22222"
    commit_id3 = "f74c808e-2388-4b0a-a051-17eb9eb33333"
    with target.engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO 'commit' VALUES "
                f"('{commit_id1}', '{pd.Timestamp.now()}', 'bar', '0', '0'),"
                f"('{commit_id2}', '{pd.Timestamp.now()}', 'bar', '0', '0'),"
                f"('{commit_id3}', '{pd.Timestamp.now()}', 'bar', '0', '0');"
            )
        )
    actual = target.get_commit(commit_num=2)
    assert actual.id == UUID(commit_id2)


def test_get_commit_when_commit_num_does_not_exist_then_error_is_raised(target):
    with pytest.raises(EntropyError):
        target.get_commit(commit_num=2)


def test_save_temp_commit_can_be_called_more_than_once_with_params(target):
    commit1 = Commit(params={"foo": Param("bar")}, tags={})
    target.save_temp_commit(commit1)
    commit2 = Commit(params={"foo": Param("baz")}, tags={})
    target.save_temp_commit(commit2)
    actual = target.load_temp_commit()
    assert actual.params["foo"].value == "baz"

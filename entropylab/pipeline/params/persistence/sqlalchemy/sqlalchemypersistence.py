import os
import uuid
from pathlib import Path
from typing import Optional, Set, List

import jsonpickle
from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from alembic.config import Config

from sqlalchemy.orm import sessionmaker

from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.params.persistence.persistence import Persistence, Commit
from entropylab.pipeline.params.persistence.sqlalchemy.model import (
    CommitTable,
    TempTable,
    Base,
)

TEMP_COMMIT_ID = "00000000-0000-0000-0000-000000000000"


class SqlAlchemyPersistence(Persistence):
    def __init__(self, url: Optional[str] = None):
        self.engine = create_engine(
            url,
            json_serializer=jsonpickle.encode,
            json_deserializer=jsonpickle.decode,
        )
        self.__session_maker = sessionmaker(bind=self.engine)
        if self._db_is_empty():
            self.__init()

    def __init(self):
        with self.engine.connect() as connection:
            alembic_cfg = self.__alembic_build_config(connection)
            command.upgrade(alembic_cfg, "head")

    def __alembic_build_config(self, connection: Connection) -> Config:
        config_location = self._abs_path_to("alembic.ini")
        script_location = self._abs_path_to("alembic")
        alembic_cfg = Config(config_location)
        alembic_cfg.set_main_option("script_location", script_location)
        """
        Modified by @urig to share a single engine across multiple (typically in-memory)
        connections, based on this cookbook recipe:
        https://alembic.sqlalchemy.org/en/latest/cookbook.html#connection-sharing
        """
        alembic_cfg.set_main_option("sqlalchemy.url", "sqlite:///:memory:")
        alembic_cfg.attributes["connection"] = connection  # overrides dummy url above
        return alembic_cfg

    @staticmethod
    def _abs_path_to(rel_path: str) -> str:
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent
        return os.path.join(source_dir, rel_path)

    def close(self):
        self.__session_maker.close_all()

    def get_commit(
        self, commit_id: Optional[str] = None, commit_num: Optional[int] = None
    ):
        if commit_id:
            with self.__session_maker() as session:
                commit = (
                    session.query(CommitTable)
                    .filter(CommitTable.id == commit_id)
                    .one_or_none()
                )
                if commit:
                    return commit
                else:
                    raise EntropyError(f"Commit with id '{commit_id}' not found")
        elif commit_num:
            with self.__session_maker() as session:
                commit = (
                    session.query(CommitTable)
                    .order_by(CommitTable.timestamp.asc())
                    .offset(commit_num - 1)
                    .limit(1)
                    .one_or_none()
                )
                if commit:
                    return commit
                else:
                    raise EntropyError(f"Commit with id '{commit_id}' not found")

        else:
            return self.get_latest_commit()

    def get_latest_commit(self):
        with self.__session_maker() as session:
            commit = (
                session.query(CommitTable)
                .order_by(CommitTable.timestamp.desc())
                .first()
            )
            return commit

    def commit(
        self,
        commit: Commit,
        dirty_keys: Optional[Set[str]] = None,
    ) -> str:
        commit.id = self.__generate_commit_id()
        # TODO: Perhaps create the timestamp here?
        self.stamp_dirty_params_with_commit(commit, dirty_keys)
        commit_table = CommitTable()
        commit_table.id = commit.id
        commit_table.timestamp = commit.timestamp
        commit_table.label = commit.label
        commit_table.params = commit.params
        commit_table.tags = commit.tags
        with self.__session_maker() as session:
            session.add(commit_table)
            session.commit()
            return commit_table.id

    @staticmethod
    def __generate_commit_id() -> str:
        return str(uuid.uuid4())

    def search_commits(
        self, label: Optional[str] = None, key: Optional[str] = None
    ) -> List[Commit]:
        with self.__session_maker() as session:
            commits = session.query(CommitTable)
            if label:
                commits = commits.filter(CommitTable.label == label)
            if key:
                commits = commits.filter(CommitTable.params.contains(key))
            return commits.all()

    def save_temp_commit(self, commit: Commit) -> None:
        temp_table = TempTable()
        # TODO: Perhaps create the timestamp here?
        temp_table.id = TEMP_COMMIT_ID
        temp_table.timestamp = commit.timestamp
        temp_table.label = commit.label
        temp_table.params = commit.params
        temp_table.tags = commit.tags
        with self.__session_maker() as session:
            session.add(temp_table)
            session.commit()

    def load_temp_commit(self) -> Commit:
        with self.__session_maker() as session:
            temp_table = (
                session.query(TempTable)
                .filter(TempTable.id == TEMP_COMMIT_ID)
                .one_or_none()
            )
            if not temp_table:
                raise EntropyError(
                    "Temp is empty. Use save_temp_commit() before using "
                    "load_temp_commit() "
                )
            else:
                commit = Commit(
                    id=temp_table.id,
                    timestamp=temp_table.timestamp,
                    label=temp_table.label,
                    params=temp_table.params,
                    tags=temp_table.tags,
                )
            return commit

    def _db_is_empty(self) -> bool:
        with self.__session_maker() as session:
            result = session.execute(
                text("SELECT sql FROM sqlite_master WHERE type = 'table'")
            )
            return len(result.fetchall()) == 0

import uuid
from typing import Optional, Set, List

import jsonpickle
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.params.persistence.persistence import Persistence, Commit
from entropylab.pipeline.params.persistence.sqlalchemy.model import CommitTable


class SqlAlchemyPersistence(Persistence):
    def __init__(self, url: Optional[str] = None):
        self.engine = create_engine(
            url,
            json_serializer=jsonpickle.encode,
            json_deserializer=jsonpickle.decode,
        )
        self.__session_maker = sessionmaker(bind=self.engine)

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
        commit_table = CommitTable(
            id=commit.id,
            timestamp=commit.timestamp,
            label=commit.label,
            params=commit.params,
            tags=commit.tags,
        )
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
            return commits.all

    def save_temp_commit(self, commit: Commit) -> None:
        pass

    def load_temp_commit(self) -> Commit:
        pass

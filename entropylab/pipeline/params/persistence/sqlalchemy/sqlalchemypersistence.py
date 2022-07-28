from typing import Optional

import jsonpickle
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.params.persistence.persistence import Persistence
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

    def commit(self, commit, label, dirty_keys):
        pass

    def search_commits(self, label, key):
        pass

    def save_temp_commit(self, commit):
        pass

    def load_temp_commit(self):
        pass

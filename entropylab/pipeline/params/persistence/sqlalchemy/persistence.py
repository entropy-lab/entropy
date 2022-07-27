from typing import Optional

import jsonpickle
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from entropylab.pipeline.params.persistence.persistence import Persistence
from entropylab.pipeline.params.persistence.sqlalchemy.model import Commit


class Persistence(Persistence):
    def __init__(self, url: Optional[str] = None):
        self.__session_maker = sessionmaker(
            bind=create_engine(
                url,
                json_serializer=jsonpickle.encode,
                json_deserializer=jsonpickle.decode,
            )
        )

    def close(self):
        self.__session_maker.close_all()

    def get_commit(self, commit_id, commit_num):
        pass

    def get_latest_commit(self):
        with self.__session_maker() as session:
            commit = session.query(Commit).order_by(Commit.timestamp.desc()).first()
            return commit

    def commit(self, commit, label, dirty_keys):
        pass

    def search_commits(self, label, key):
        pass

    def save_temp_commit(self, commit):
        pass

    def load_temp_commit(self):
        pass

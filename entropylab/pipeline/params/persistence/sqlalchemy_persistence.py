from entropylab.pipeline.params.persistence.persistence import Persistence


class SqlAlchemyPersistence(Persistence):
    def close(self):
        pass

    def get_commit(self, commit_id, commit_num):
        pass

    def get_latest_commit(self):
        pass

    def commit(self, commit, label, dirty_keys):
        pass

    def search_commits(self, label, key):
        pass

    def save_temp_commit(self, commit):
        pass

    def load_temp_commit(self):
        pass

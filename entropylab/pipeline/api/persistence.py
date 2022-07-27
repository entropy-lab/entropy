from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ContextManager


class Persistence(ABC, ContextManager):
    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def get_commit(self, commit_id, commit_num):
        pass

    @abstractmethod
    def get_latest_commit(self):
        pass

    @abstractmethod
    def commit(self, commit, label, dirty_keys):
        pass

    @abstractmethod
    def search_commits(self, label, key):
        pass

    @abstractmethod
    def save_temp_commit(self, commit):
        pass

    @abstractmethod
    def load_temp_commit(self):
        pass

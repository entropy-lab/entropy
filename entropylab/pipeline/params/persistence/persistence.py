from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional

from entropylab.pipeline.api.param_store import _ns_to_datetime


class Persistence(ABC):
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


@dataclass
class Commit:
    params: Dict
    tags: Dict
    id: Optional[str] = None  # commit_id
    timestamp: Optional[int] = None  # nanoseconds since epoch
    label: Optional[str] = None

    def __post_init__(self):
        self.id = self.id or ""
        self.timestamp = self.timestamp or time.time_ns()
        self.label = self.label or None
        self.params = self.params or {}
        self.tags = self.tags or {}


class Metadata:
    def __init__(self, d: Dict = None):
        self.id: str = ""  # commit_id
        self.timestamp: int = time.time_ns()  # nanoseconds since epoch
        self.label: Optional[str] = None
        if d:
            self.__dict__.update(d)

    def __repr__(self) -> str:
        d = self.__dict__.copy()
        d["timestamp"] = _ns_to_datetime(self.timestamp)
        return f"<Metadata({_dict_to_json(d)})>"

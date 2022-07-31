from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Optional, Set, List

import pandas as pd


class Persistence(ABC):
    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def get_commit(
        self, commit_id: Optional[str] = None, commit_num: Optional[int] = None
    ):
        pass

    @abstractmethod
    def get_latest_commit(self):
        pass

    @abstractmethod
    def commit(
        self,
        commit: Commit,
        dirty_keys: Optional[Set[str]] = None,
    ) -> str:
        pass

    @abstractmethod
    def search_commits(
        self, label: Optional[str] = None, key: Optional[str] = None
    ) -> List[Commit]:
        pass

    @abstractmethod
    def save_temp_commit(self, commit):
        pass

    @abstractmethod
    def load_temp_commit(self):
        pass

    @staticmethod
    def stamp_dirty_params_with_commit(
        commit: Commit, dirty_keys: Optional[Set[str]] = None
    ):
        if dirty_keys:
            for key in dirty_keys:
                if key in commit.params:
                    param = commit.params[key]
                    param.commit_id = commit.id
                    if isinstance(param.expiration, timedelta):
                        param.expiration = commit.timestamp + param.expiration


@dataclass
class Commit:
    params: Dict
    tags: Dict
    id: Optional[str] = None  # commit_id
    timestamp: Optional[pd.Timestamp] = None
    label: Optional[str] = None

    def __post_init__(self):
        self.id = self.id or ""
        self.timestamp = self.timestamp or pd.Timestamp(time.time_ns())
        self.label = self.label or None
        self.params = self.params or {}
        self.tags = self.tags or {}

    def to_metadata(self) -> Metadata:
        return Metadata(self.id, self.timestamp, self.label)


@dataclass
class Metadata:
    id: str  # commit id
    timestamp: pd.Timestamp
    label: Optional[str] = None

    def __post_init__(self):
        self.id = self.id or ""
        self.timestamp = self.timestamp or pd.Timestamp(time.time_ns())

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum, unique
from typing import Dict, List, Optional

import pandas as pd


@unique
class MergeStrategy(Enum):
    OURS = 1
    THEIRS = 2


# TODO: Derive from MutableMapping (-> rename get() to get_value())
class ParamStore(ABC):
    @abstractmethod
    def keys(self):
        pass

    @abstractmethod
    def to_dict(self):
        pass

    @abstractmethod
    def get(self, key, commit_id):
        """
            returns the value of a param by key

        :param key: the key identifying the param
        :param commit_id: an optional commit_id. if provided, the value will be
        returned from the specified commit
        """
        pass

    @abstractmethod
    def rename_key(self, key: str, new_key: str):
        pass

    @abstractmethod
    def commit(self, label):
        pass

    @abstractmethod
    def checkout(self, commit_id: str, commit_num: int = None, move_by: int = None):
        pass

    @abstractmethod
    def list_commits(self, label: Optional[str]):
        """
            returns a list of commits

        :param label: an optional label, if given then only commits that match
        it will be returned
        """
        pass

    @abstractmethod
    def list_values(self, key: str) -> pd.DataFrame:
        """
            list all the values of a given key taken from commit history,
            sorted by date ascending

        :param key: the key for which to list values
        :returns: a list of tuples where the values are, in order:
            - the value of the key
            - time of commit
            - commit_id
            - label assigned to commit
        """
        pass

    @abstractmethod
    def merge(
        self,
        theirs: Dict | ParamStore,
        merge_strategy: Optional[MergeStrategy] = MergeStrategy.OURS,
    ) -> None:
        pass

    """ Tags """

    @abstractmethod
    def add_tag(self, tag: str, key: str) -> None:
        pass

    @abstractmethod
    def remove_tag(self, tag: str, key: str) -> None:
        pass

    @abstractmethod
    def list_keys_for_tag(self, tag: str) -> List[str]:
        pass

    @abstractmethod
    def list_tags_for_key(self, key: str):
        pass

    """ Temporary State """

    @abstractmethod
    def save_temp(self) -> None:
        pass

    @abstractmethod
    def load_temp(self) -> None:
        pass

    @property
    @abstractmethod
    def is_dirty(self):
        """True iff params have been changed since the store has last been
        initialized or checked out"""
        pass

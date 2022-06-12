from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum, unique
from typing import Dict, List, Optional, MutableMapping

import pandas as pd


@unique
class MergeStrategy(Enum):
    OURS = 1
    THEIRS = 2


class ParamStore(ABC, MutableMapping):
    @abstractmethod
    def keys(self):
        pass

    @abstractmethod
    def to_dict(self):
        pass

    @abstractmethod
    def get_value(self, key: str, commit_id: Optional[str] = None) -> object:
        """
        Returns the value of a param by its key

        :param key: the key identifying the param
        :param commit_id: an optional commit_id. If provided, the value will be
        returned from the specified commit
        """
        pass

    @abstractmethod
    def get_param(self, key: str, commit_id: Optional[str] = None) -> Param:
        """
        Returns a copy of the Param instance of a value stored in ParamStore

        :param key: the key identifying the param
        :param commit_id: an optional commit_id. If provided, the Param will be
        returned from the specified commit
        """
        pass

    @abstractmethod
    def set_param(self, key: str, value: object, expiration: Optional[timedelta]):
        """
        Sets a Param in the ParamStore

        :param key: the key identifying the param
        :param value: the value of the param
        :param key: an optional period of time (measured from the time the param
        is committed) after which the prime expires (i.e. param.has_expired
        returns True)
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
        Returns a list of commits

        :param label: an optional label, if given then only commits that match
        it will be returned
        """
        pass

    @abstractmethod
    def list_values(self, key: str) -> pd.DataFrame:
        """
        Lists all the values of a given key taken from commit history,
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
        """
        True iff params have been changed since the store has last been
        initialized or checked out"""
        pass


class Param(Dict):
    def __init__(self, value):
        super().__init__()
        self.value: object = value
        self.commit_id: Optional[str] = None
        self.expiration: Optional[timedelta | int] = None

    def __repr__(self):
        return (
            f"<Param(value={self.value}, "
            f"commit_id={self.commit_id}, "
            f"expiration={self.__expiration_repr})> "
        )

    @property
    def __expiration_repr(self):
        if isinstance(self.expiration, int):
            return _ns_to_datetime(self.expiration)
        else:
            return False

    @property
    def has_expired(self):
        """
        Indicates whether the Param value has expired. Returns True iff the Param
        has been committed and the time elapsed since the commit operation has exceeded
        the time recorded in the `expiration` property"""
        if isinstance(self.expiration, int):
            return self.expiration < time.time_ns()
        else:
            return False


def _ns_to_datetime(ns: int) -> pd.datetime:
    """
    Converts a UNIX epoch timestamp in nano-seconds to a human readable string"""
    return pd.to_datetime(ns)

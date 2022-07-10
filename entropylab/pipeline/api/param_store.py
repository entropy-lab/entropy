from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum, unique
from typing import Dict, List, Optional, MutableMapping

import pandas as pd

LOCAL_TZ = datetime.now().astimezone().tzinfo


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

    @abstractmethod
    def diff(
        self, old_commit_id: Optional[str] = None, new_commit_id: Optional[str] = None
    ) -> Dict[str, Dict]:
        """Shows the difference in Param values between two commits.

        :param old_commit_id: The id of the first ("older") commit to compare. If
            None, or not specified, defaults to the latest commit id.
        :param new_commit_id: The id of the second  ("newer") commit to compare. If
            None, or not specified, defaults to the current state of the store (incl.
             "dirty" values)
        :return: A dictionary where keys are the keys of params whose values have
            changed. Dictionary values indicate the `old_value` of the param and the
            `new_value` of the param. A new param will only show the `new_value`. A
            deleted param will only show the `old_value`.
            Example: {"foo": {"old_value": "bar", "new_value": "baz"}}
        """
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
        self.description: Optional[str] = None
        self.node_id: Optional[str] = None

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
    """Convert a UNIX epoch timestamp in nano-seconds to pandas Timestamp in local TZ"""
    return pd.to_datetime(ns, utc=True).tz_convert(LOCAL_TZ)

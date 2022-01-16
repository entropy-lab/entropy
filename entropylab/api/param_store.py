import hashlib
import json
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum, unique
from typing import Dict, List, Any, Optional

from tinydb import TinyDB, Query
from tinydb.storages import MemoryStorage

from entropylab.api.errors import EntropyError


@unique
class MergeStrategy(Enum):
    OURS = (1,)
    THEIRS = (2,)
    RECURSIVE = 3


class Commit:
    id: str
    datetime: datetime


class ParamStore(ABC):
    def __init__(self):
        super().__init__()

    """ Present dictionary """

    @abstractmethod
    def __getitem__(self, key: str) -> Any:
        pass

    @abstractmethod
    def __setitem__(self, key: str, value: Any) -> None:
        pass

    @abstractmethod
    def commit(self) -> str:
        pass

    @abstractmethod
    def to_dict(self) -> Dict:
        pass

    # def merge(
    #     self,
    #     theirs: ParamStore,
    #     merge_strategy: Optional[MergeStrategy] = MergeStrategy.OURS,
    # ) -> None:
    #     pass

    def search_for_label(self, label: str) -> List[Commit]:
        pass


class InProcessParamStore(ParamStore):

    """Naive implementation of ParamStore based on tinydb

    Important:
    Using this implementation in multiple concurrent processes is not supported.
    """

    # TODO: Use path to entropy project instead of direct path to tinydb file?
    def __init__(self, path: Optional[str] = None):
        super().__init__()
        self._dict = dict()
        if path is None:
            self._db = TinyDB(storage=MemoryStorage)
        else:
            self._db = TinyDB(path)

    """ Present dictionary """

    def __setitem__(self, key: str, value: Any) -> None:
        self._dict[key] = value

    def __getitem__(self, key: str) -> Any:
        return self._dict[key]

    def __contains__(self, key: str) -> bool:
        return key in self._dict

    def to_dict(self) -> Dict:
        return dict(self._dict)

    """ Commits """

    def commit(self) -> str:
        commit_id = self._hash_dict()
        # TODO: Protect against commit the same hash twice:
        self._db.insert({"_id": commit_id} | self._dict)
        return commit_id

    def checkout(self, commit_id: str):
        commit_dict = self._get_commit_dict(commit_id)
        self._dict = commit_dict

    def _hash_dict(self):
        dict_as_string = json.dumps(
            self._dict, sort_keys=True, ensure_ascii=True
        ).encode("utf-8")
        return hashlib.sha1(dict_as_string).hexdigest()

    def _get_commit_dict(self, commit_id: str) -> Dict:
        query = Query()
        # noinspection PyProtectedMember
        result = self._db.search(query._id == commit_id)
        if len(result) == 0:
            raise EntropyError(f"Commit with id '{commit_id}' not found")
        if len(result) == 0:
            raise EntropyError(
                f"{len(result)} commits with id '{commit_id}' found. "
                f"Only one commit is allowed per id"
            )
        del result[0]["_id"]
        return result[0]

import hashlib
import json
import time
from abc import ABC, abstractmethod
from enum import Enum, unique
from typing import Dict, List, Any, Optional

from tinydb import TinyDB, Query
from tinydb.storages import MemoryStorage

from entropylab.api.errors import EntropyError


@unique
class MergeStrategy(Enum):
    OURS = (1,)
    THEIRS = (2,)
    BOTH = 3


class Header:
    id: str
    ns: int


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

    def search_for_label(self, label: str) -> List[Header]:
        pass


class InProcessParamStore(ParamStore):

    """Naive implementation of ParamStore based on tinydb

    Important:
    Using this implementation in multiple concurrent processes is not supported.
    """

    # TODO: Use path to entropy project instead of direct path to tinydb file?
    def __init__(self, path: Optional[str] = None):
        super().__init__()
        self._body = dict()
        if path is None:
            self._db = TinyDB(storage=MemoryStorage)
        else:
            self._db = TinyDB(path)

    """ Present dictionary """

    def __setitem__(self, key: str, value: Any) -> None:
        self._body[key] = value

    def __getitem__(self, key: str) -> Any:
        return self._body[key]

    def __delitem__(self, *args, **kwargs):
        self._body.__delitem__(*args, **kwargs)

    def __contains__(self, key: str) -> bool:
        return key in self._body

    def to_dict(self) -> Dict:
        return dict(self._body)

    def get(self, key: str, commit_id: Optional[str] = None):
        if commit_id is None:
            return self[key]
        else:
            commit_dict = self._get_commit_body(commit_id)
            return commit_dict[key]

    """ Commits """

    def commit(self) -> str:
        id = self._hash_dict()
        if self._db.contains(Query().header.id == id):
            # TODO: log a warning?
            return id
        else:
            header = dict(id=id, ns=time.time_ns())
            self._db.insert(dict(header=header, body=self._body))
            return header["id"]

    def checkout(self, commit_id: str):
        commit_dict = self._get_commit_body(commit_id)
        self._body = commit_dict

    def _hash_dict(self):
        dict_as_string = json.dumps(
            self._body, sort_keys=True, ensure_ascii=True
        ).encode("utf-8")
        return hashlib.sha1(dict_as_string).hexdigest()

    def _get_commit_body(self, commit_id: str) -> Dict:
        query = Query()
        # noinspection PyProtectedMember
        result = self._db.search(query.header.id == commit_id)
        # validate
        if len(result) == 0:
            raise EntropyError(f"Commit with id '{commit_id}' not found")
        if len(result) == 0:
            raise EntropyError(
                f"{len(result)} commits with id '{commit_id}' found. "
                f"Only one commit is allowed per id"
            )
        return result[0]["body"]

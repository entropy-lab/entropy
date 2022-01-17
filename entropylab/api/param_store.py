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


class Metadata:
    id: str
    ns: int

    def __repr__(self) -> str:
        return json.dumps(
            self, default=lambda o: o.__dict__, sort_keys=True, ensure_ascii=True
        )

    def to_dict(self) -> dict:
        return dict(id=self.id, ns=self.ns)


class ParamStore(ABC):
    def __init__(self):
        super().__init__()

    """ Params dictionary """

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

    def search_for_label(self, label: str) -> List[Metadata]:
        pass


class InProcessParamStore(ParamStore):

    """Naive implementation of ParamStore based on tinydb

    Important:
    Using this implementation in multiple concurrent processes is not supported.
    """

    # TODO: Use path to entropy project instead of direct path to tinydb file?
    def __init__(self, path: Optional[str] = None):
        super().__init__()
        self._is_dirty = True  # were params modified since commit() / checkout()?
        self._base_commit_id = None  # id of last commit checked out/committed
        self._params = dict()
        if path is None:
            self._db = TinyDB(storage=MemoryStorage)
        else:
            self._db = TinyDB(path)

    """ Attributes """

    def __setattr__(self, name, value):
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            self._params[name] = value
            self._is_dirty = True

    def __getattr__(self, name):
        if name.startswith("_"):
            return super().__getattribute__(name)
        else:
            return self._params[name]

    def __delattr__(self, name):
        if name.startswith("_"):
            super().__delattr__(name)
        else:
            del self._params[name]
            self._is_dirty = True

    """ Items """

    def __setitem__(self, key: str, value: Any) -> None:
        self._params[key] = value
        self._is_dirty = True

    def __getitem__(self, key: str) -> Any:
        return self._params[key]

    def __delitem__(self, *args, **kwargs):
        self._params.__delitem__(*args, **kwargs)
        self._is_dirty = True

    def __contains__(self, key: str) -> bool:
        return key in self._params

    def to_dict(self) -> Dict:
        return dict(self._params)

    def get(self, key: str, commit_id: Optional[str] = None):
        if commit_id is None:
            return self[key]
        else:
            commit_dict = self._get_commit_params(commit_id)
            return commit_dict[key]

    """ Commits """

    def commit(self) -> str:
        if not self._is_dirty:
            return self._base_commit_id
        metadata = self._generate_metadata()
        self._db.insert(dict(metadata=metadata.to_dict(), params=self._params))
        self._base_commit_id = metadata.id
        self._is_dirty = False
        return metadata.id

    def checkout(self, commit_id: str):
        commit_dict = self._get_commit_params(commit_id)
        self._params = commit_dict
        self._base_commit_id = commit_id
        self._is_dirty = False

    def _generate_metadata(self) -> (str, int):
        metadata = Metadata()
        metadata.ns = time.time_ns()
        jzon = json.dumps(self._params, sort_keys=True, ensure_ascii=True)
        bytez = (jzon + str(metadata.ns)).encode("utf-8")
        metadata.id = hashlib.sha1(bytez).hexdigest()
        return metadata

    def _get_commit_params(self, commit_id: str) -> Dict:
        query = Query()
        # noinspection PyProtectedMember
        result = self._db.search(query.metadata.id == commit_id)
        # validate
        if len(result) == 0:
            raise EntropyError(f"Commit with id '{commit_id}' not found")
        if len(result) == 0:
            raise EntropyError(
                f"{len(result)} commits with id '{commit_id}' found. "
                f"Only one commit is allowed per id"
            )
        return result[0]["params"]

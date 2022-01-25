from __future__ import annotations

import hashlib
import json
import time
from abc import ABC, abstractmethod
from enum import Enum, unique
from typing import Dict, List, Any, Optional

import pandas as pd
from tinydb import TinyDB, Query
from tinydb.storages import MemoryStorage
from tinydb.table import Document

from entropylab.api.errors import EntropyError


@unique
class MergeStrategy(Enum):
    OURS = 1
    THEIRS = 2


class Metadata:
    # TODO: Proper public props
    id: str
    ns: int
    label: Optional[str]

    def __init__(self, dikt=None):
        if dikt:
            self.__dict__.update(dikt)

    def __repr__(self) -> str:
        date = pd.to_datetime(self.ns).to_pydatetime()
        return f"<Metadata(id='{self.id}', ns='{date}', label='{self.label}')>"
        # return json.dumps(
        #     self, default=lambda o: o.__dict__, sort_keys=True, ensure_ascii=True
        # )


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


class InProcessParamStore(ParamStore):

    """Naive implementation of ParamStore based on tinydb

    Important:
    Using this implementation in multiple concurrent processes is not supported.
    """

    # TODO: Use path to entropy project instead of direct path to tinydb file?
    def __init__(
        self,
        path: Optional[str] = None,
        theirs: Optional[Dict | ParamStore] = None,
        merge_strategy: Optional[MergeStrategy] = MergeStrategy.THEIRS,
    ):
        super().__init__()
        self._is_dirty = True  # were params modified since commit() / checkout()?
        self._base_commit_id = None  # id of last commit checked out/committed
        self._base_doc_id = None
        self._params = dict()
        if path is None:
            self._db = TinyDB(storage=MemoryStorage)
        else:
            self._db = TinyDB(path)
        if theirs is not None:
            self.merge(theirs, merge_strategy)

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
            return self._params.get(key)
        else:
            commit = self._get_commit(commit_id)
            return commit["params"][key]

    """ Commits """

    def commit(self, label: Optional[str] = None) -> str:
        if not self._is_dirty:
            return self._base_commit_id
        metadata = self._generate_metadata(label)

        doc_id = self._db.insert(dict(metadata=metadata.__dict__, params=self._params))
        self._base_commit_id = metadata.id
        self._base_doc_id = doc_id
        self._is_dirty = False
        return metadata.id

    def checkout(
        self,
        commit_id: Optional[str] = None,
        commit_num: Optional[int] = None,
        move_by: Optional[int] = None,
    ) -> None:
        commit = self._get_commit(commit_id, commit_num, move_by)
        self._params = commit["params"]
        self._base_commit_id = commit_id
        self._base_doc_id = commit.doc_id
        self._is_dirty = False

    def log(self, label: Optional[str] = None) -> List[Metadata]:
        documents = self._db.search(
            Query().metadata.label.test(_test_if_value_contains(label))
        )
        metadata = map(_extract_metadata, documents)
        return list(metadata)

    def _generate_metadata(self, label: Optional[str] = None) -> (str, int):
        metadata = Metadata()
        metadata.ns = time.time_ns()
        jzon = json.dumps(self._params, sort_keys=True, ensure_ascii=True)
        bytez = (jzon + str(metadata.ns)).encode("utf-8")
        metadata.id = hashlib.sha1(bytez).hexdigest()
        metadata.label = label
        return metadata

    def _get_commit(
        self,
        commit_id: Optional[str] = None,
        commit_num: Optional[int] = None,
        move_by: Optional[int] = None,
    ) -> Document:
        query = Query()
        # noinspection PyProtectedMember
        if commit_id is not None:
            commit = self._get_commit_by_id(commit_id)
        elif commit_num is not None:
            commit = self._get_commit_by_num(commit_num)
        elif move_by is not None:
            commit = self._get_commit_by_move_by(move_by)
        else:
            raise EntropyError(
                "Please provide one of the following arguments: "
                "commit_id, commit_num, or move_by"
            )
        return commit

    def _get_commit_by_id(self, commit_id: str):
        result = self._db.search(Query().metadata.id == commit_id)
        if len(result) == 0:
            raise EntropyError(f"Commit with id '{commit_id}' not found")
        if len(result) > 1:
            raise EntropyError(
                f"{len(result)} commits with id '{commit_id}' found. "
                f"Only one commit is allowed per id"
            )
        return result[0]

    def _get_commit_by_num(self, commit_num: int):
        result = self._db.get(doc_id=commit_num)
        if result is None:
            raise EntropyError(f"Commit with number '{commit_num}' not found")
        return result

    def _get_commit_by_move_by(self, move_by: int):
        doc_id = self._base_doc_id + move_by
        result = self._db.get(doc_id=doc_id)
        return result

    """ Merge """

    def merge(
        self,
        theirs: Dict | ParamStore,
        merge_strategy: Optional[MergeStrategy] = MergeStrategy.OURS,
    ) -> None:
        if issubclass(type(theirs), ParamStore):
            theirs = theirs.to_dict()
        if merge_strategy == MergeStrategy.OURS:
            _merge_trees(self._params, theirs)
        if merge_strategy == MergeStrategy.THEIRS:
            theirs_copy = dict(theirs)
            _merge_trees(theirs_copy, self._params)
            self._params = theirs_copy


def _test_if_value_contains(label: str):
    return lambda val: (label or "") in (val or "")


def _extract_metadata(document: Document):
    return Metadata(document.get("metadata"))


def _merge_trees(a: Dict, b: Dict, path: List[str] = None):
    """Merges b into a - Copy pasted from https://stackoverflow.com/a/7205107/33404"""
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                _merge_trees(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value, nothing to do
            else:
                pass  # conflict, ignore b
        else:
            a[key] = b[key]  # "copy" from b to a
    return a

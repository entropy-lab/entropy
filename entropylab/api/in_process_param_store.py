from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import MutableMapping
from datetime import datetime
from typing import Optional, Dict, Any, List, Iterator, Callable

import pandas as pd
from tinydb import TinyDB, Query
from tinydb.storages import MemoryStorage
from tinydb.table import Document

from entropylab.api.errors import EntropyError
from entropylab.api.param_store import ParamStore, MergeStrategy


class InProcessParamStore(ParamStore, MutableMapping):

    """Naive implementation of ParamStore based on tinydb

    Important:
    Using this implementation in multiple concurrent processes is not supported.
    """

    def __init__(
        self,
        path: Optional[str] = None,
        theirs: Optional[Dict | ParamStore] = None,
        merge_strategy: Optional[MergeStrategy] = MergeStrategy.THEIRS,
    ):

        super().__init__()

        self._lock = threading.Lock()
        self._base_commit_id: Optional[str] = None  # last commit checked out/committed
        self._base_doc_id: Optional[int] = None  # tinydb document id of last commit...
        self._params: Dict[str, Any] = dict()  # where current params are stored
        self._tags: Dict[str, List[str]] = dict()  # tags that are mapped to keys
        self._is_dirty: bool = True  # can the store be committed at this time?

        if path is None:
            self._is_in_memory_mode = True
            self._db = TinyDB(storage=MemoryStorage)
        else:
            self._is_in_memory_mode = False
            self._db = TinyDB(path)
        if theirs is not None:
            self.merge(theirs, merge_strategy)

    """ Properties """

    @property
    def is_dirty(self):
        with self._lock:
            return self._is_dirty

    """ MutableMapping """

    def __iter__(self) -> Iterator[Any]:
        with self._lock:
            return self._params.__iter__()

    def __len__(self) -> int:
        with self._lock:
            return self._params.__len__()

    def __contains__(self, key: str) -> bool:
        with self._lock:
            return key in self._params

    def __setattr__(self, name, value):
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            with self._lock:
                self._params[name] = value
                self._is_dirty = True

    def __getattr__(self, name):
        if name.startswith("_"):
            return super().__getattribute__(name)
        else:
            with self._lock:
                return self._params[name]

    def __delattr__(self, name):
        if name.startswith("_"):
            super().__delattr__(name)
        else:
            with self._lock:
                del self._params[name]
                self._remove_key_from_tags(name)
                self._is_dirty = True

    def __setitem__(self, key: str, value: Any) -> None:
        if key.startswith("_"):
            raise KeyError(
                f"ParamStore keys cannot start with underscore (_). Key: {key}"
            )
        else:
            with self._lock:
                self._params[key] = value
                self._is_dirty = True

    def __getitem__(self, key: str) -> Any:
        with self._lock:
            return self._params[key]

    def __delitem__(self, *args, **kwargs):
        with self._lock:
            self._params.__delitem__(*args, **kwargs)
            self._remove_key_from_tags(args[0])
            self._is_dirty = True

    def to_dict(self) -> Dict:
        with self._lock:
            return dict(self._params)

    def get(self, key: str, commit_id: Optional[str] = None):
        with self._lock:
            if commit_id is None:
                return self._params.get(key)
            else:
                commit = self._get_commit(commit_id)
                return commit["params"][key]

    def _remove_key_from_tags(self, key: str):
        for tag in self._tags:
            if key in self._tags[tag]:
                self._tags[tag].remove(key)

    """ Commits """

    def commit(self, label: Optional[str] = None) -> str:
        with self._lock:
            if not self._is_dirty:
                return self._base_commit_id
            metadata = self._generate_metadata(label)
            if self._is_in_memory_mode:
                params = _deep_copy(self._params)
            else:
                params = self._params
            doc_id = self._db.insert(
                dict(metadata=metadata.__dict__, params=params, tags=self._tags)
            )
            self._base_commit_id = metadata.id
            self._base_doc_id = doc_id
            self._is_dirty = False
            return metadata.id

    def _generate_metadata(self, label: Optional[str] = None) -> Metadata:
        metadata = Metadata()
        metadata.ns = time.time_ns()
        params_json = json.dumps(self._params, sort_keys=True, ensure_ascii=True)
        commit_encoded = (params_json + str(metadata.ns)).encode("utf-8")
        metadata.id = hashlib.sha1(commit_encoded).hexdigest()
        metadata.label = label
        return metadata

    def checkout(
        self,
        commit_id: Optional[str] = None,
        commit_num: Optional[int] = None,
        move_by: Optional[int] = None,
    ) -> None:
        with self._lock:
            commit = self._get_commit(commit_id, commit_num, move_by)
            self._params = commit["params"]
            self._tags = commit["tags"]
            self._base_commit_id = commit_id
            self._base_doc_id = commit.doc_id
            self._is_dirty = False

    def list_commits(self, label: Optional[str] = None) -> List[Metadata]:
        with self._lock:
            documents = self._db.search(
                Query().metadata.label.test(_test_if_value_contains(label))
            )
            metadata = map(_extract_metadata, documents)
            return list(metadata)

    def _get_commit(
        self,
        commit_id: Optional[str] = None,
        commit_num: Optional[int] = None,
        move_by: Optional[int] = None,
    ) -> Document:
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
        with self._lock:
            self._merge_trees(self._params, theirs, merge_strategy)

    def _merge_trees(self, a: Dict, b: Dict, merge_strategy: MergeStrategy) -> Dict:
        """Merges b into a - Copy pasted from https://stackoverflow.com/a/7205107/33404"""
        for key in b:
            if key in a:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    self._merge_trees(a[key], b[key], merge_strategy)
                elif a[key] == b[key]:
                    pass  # same leaf value, nothing to do
                else:  # conflict:
                    if merge_strategy == MergeStrategy.OURS:
                        pass  # a takes precedence, ignore b
                    elif merge_strategy == MergeStrategy.THEIRS:
                        a[key] = b[key]  # b takes precedence, overwrite a
                        self._is_dirty = True
                    else:
                        raise NotImplementedError(
                            f"MergeStrategy '{merge_strategy}' is not implemented"
                        )

            else:  # key from b is not in a:
                a[key] = b[key]  # "copy" from b to a
                self._is_dirty = True
        return a

    def list_values(self, key: str) -> pd.DataFrame:
        with self._lock:
            values = []
            commits = self._db.all()
            commits.sort(key=lambda x: x["metadata"]["ns"])
            for commit in commits:
                try:
                    value = (
                        commit["params"][key],
                        _ns_to_datetime(commit["metadata"]["ns"]),
                        commit["metadata"]["id"],
                        commit["metadata"]["label"],
                    )
                    values.append(value)
                except KeyError:
                    pass
            if key in self._params:
                values.append((self._params[key], None, None, None))
            df = pd.DataFrame(values)
            if not df.empty:
                df.columns = ["value", "time", "commit_id", "label"]
            return df

    """ Tags """

    def add_tag(self, tag: str, key: str) -> None:
        with self._lock:
            if key not in self._params:
                raise KeyError(f"key '{key}' is not in store")
            if tag not in self._tags:
                self._tags[tag] = []
            self._tags[tag].append(key)
            self._is_dirty = True

    def remove_tag(self, tag: str, key: str) -> None:
        with self._lock:
            if tag not in self._tags:
                return
            if key not in self._tags[tag]:
                return
            self._tags[tag].remove(key)
            self._is_dirty = True

    def list_keys(self, tag: str) -> List[str]:
        with self._lock:
            if tag not in self._tags:
                return []
            else:
                return self._tags[tag]


class Metadata:
    id: str
    ns: int
    label: Optional[str]

    def __init__(self, d: Dict = None):
        if d:
            self.__dict__.update(d)

    def __repr__(self) -> str:
        d = self.__dict__.copy()
        d["ns"] = _ns_to_datetime(self.ns)
        return f"<Metadata({_dict_to_json(d)})>"


""" Static helper methods """


def _deep_copy(d: Dict) -> Dict:
    return json.loads(json.dumps(d))


def _test_if_value_contains(label: str) -> Callable:
    return lambda val: (label or "") in (val or "")


def _extract_metadata(document: Document) -> Metadata:
    return Metadata(document.get("metadata"))


def _dict_to_json(d: Dict) -> str:
    return json.dumps(d, default=_json_dumps_default, sort_keys=True, ensure_ascii=True)


def _json_dumps_default(value):
    if isinstance(value, datetime):
        return str(value)
    else:
        return value.__dict__


def _ns_to_datetime(ns: int) -> datetime:
    """Convert a UNIX epoch timestamp in nano-seconds to a human readable string"""
    return pd.to_datetime(ns).to_pydatetime()

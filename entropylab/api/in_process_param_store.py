from __future__ import annotations

import hashlib
import json
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable

import pandas as pd
from munch import Munch
from tinydb import TinyDB, Query
from tinydb.storages import MemoryStorage
from tinydb.table import Document

from entropylab.api.errors import EntropyError
from entropylab.api.param_store import ParamStore, MergeStrategy


class InProcessParamStore(ParamStore, Munch):

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

        self.__lock = threading.RLock()
        self.__base_commit_id: Optional[str] = None  # last commit checked out/committed
        self.__base_doc_id: Optional[int] = None  # tinydb document id of last commit...
        self.__tags: Dict[str, List[str]] = dict()  # tags that are mapped to keys
        self.__is_dirty: bool = True  # can the store be committed at this time?

        if path is None:
            self.__is_in_memory_mode = True
            self.__db = TinyDB(storage=MemoryStorage)
        else:
            self.__is_in_memory_mode = False
            self.__db = TinyDB(path)
        if theirs is not None:
            self.merge(theirs, merge_strategy)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.__db.close()

    """ Properties """

    @property
    def is_dirty(self):
        with self.__lock:
            return self.__is_dirty

    """ MutableMapping """

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Set self[key] to value. The key-value pair becomes a "param" and
        can be persisted using `commit()` and retrieved later using
        `checkout()`.

        Note: Keys should not start with a dunder (`__`). Such keys are not
        treated as params and are not persisted when `commit()` is called.
        """

        if key.startswith("__") or key.startswith(f"_{self.__class__.__name__}__"):
            # keys that are private attributes are not params and are treated
            # as regular object attributes
            object.__setattr__(self, key, value)
        else:
            with self.__lock:
                super().__setitem__(key, value)
                self.__is_dirty = True

    def __getitem__(self, key: str) -> Any:
        with self.__lock:
            return super().__getitem__(key)

    def __delitem__(self, *args, **kwargs):
        with self.__lock:
            super().__delitem__(*args, **kwargs)
            self.__remove_key_from_tags(args[0])
            self.__is_dirty = True

    def to_dict(self) -> Dict:
        with self.__lock:
            return super().toDict()

    def get(self, key: str, commit_id: Optional[str] = None):
        with self.__lock:
            if commit_id is None:
                return super().__getitem__(key)
            else:
                commit = self.__get_commit(commit_id)
                return commit["params"][key]

    def __remove_key_from_tags(self, key: str):
        for tag in self.__tags:
            if key in self.__tags[tag]:
                self.__tags[tag].remove(key)

    def rename_key(self, key: str, new_key: str):
        with self.__lock:
            if new_key in self.keys():
                raise KeyError(
                    f"Cannot rename key '{key}' to key '{new_key}' because it already exists"
                )
            self.__rename_key_in_tags(key, new_key)
            value = self.__getitem__(key)
            self.__setitem__(new_key, value)
            self.__delitem__(key)

    def __rename_key_in_tags(self, key, new_key):
        for item in self.__tags.items():
            tag = item[0]
            keys = item[1]
            if key in keys:
                self.__tags[tag].remove(key)
                self.__tags[tag].append(new_key)

    """ Commits """

    def commit(self, label: Optional[str] = None) -> str:
        with self.__lock:
            if not self.__is_dirty:
                return self.__base_commit_id
            doc = self.__build_document(label)
            doc_id = self.__db.insert(doc)
            self.__base_commit_id = doc["metadata"]["id"]
            self.__base_doc_id = doc_id
            self.__is_dirty = False
            return doc["metadata"]["id"]

    def __build_document(self, label: Optional[str] = None) -> dict:
        metadata = self.__build_metadata(label)
        params = self.toDict()
        return Munch(metadata=metadata.__dict__, params=params, tags=self.__tags)

    def __build_metadata(self, label: Optional[str] = None) -> Metadata:
        metadata = Metadata()
        metadata.ns = time.time_ns()
        params_json = json.dumps(self.toDict(), sort_keys=True, ensure_ascii=True)
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
        with self.__lock:
            commit = self.__get_commit(commit_id, commit_num, move_by)
            self.clear()
            self.update(commit["params"])
            self.__tags = commit["tags"]
            self.__base_commit_id = commit_id
            self.__base_doc_id = commit.doc_id
            self.__is_dirty = False

    def list_commits(self, label: Optional[str] = None) -> List[Metadata]:
        with self.__lock:
            documents = self.__db.search(
                Query().metadata.label.test(_test_if_value_contains(label))
            )
            metadata = map(_extract_metadata, documents)
            return list(metadata)

    def __get_commit(
        self,
        commit_id: Optional[str] = None,
        commit_num: Optional[int] = None,
        move_by: Optional[int] = None,
    ) -> Document:
        if commit_id is not None:
            commit = self.__get_commit_by_id(commit_id)
        elif commit_num is not None:
            commit = self.__get_commit_by_num(commit_num)
        elif move_by is not None:
            commit = self.__get_commit_by_move_by(move_by)
        else:
            raise EntropyError(
                "Please provide one of the following arguments: "
                "commit_id, commit_num, or move_by"
            )
        return commit

    def __get_commit_by_id(self, commit_id: str):
        result = self.__db.search(Query().metadata.id == commit_id)
        if len(result) == 0:
            raise EntropyError(f"Commit with id '{commit_id}' not found")
        if len(result) > 1:
            raise EntropyError(
                f"{len(result)} commits with id '{commit_id}' found. "
                f"Only one commit is allowed per id"
            )
        return result[0]

    def __get_commit_by_num(self, commit_num: int):
        result = self.__db.get(doc_id=commit_num)
        if result is None:
            raise EntropyError(f"Commit with number '{commit_num}' not found")
        return result

    def __get_commit_by_move_by(self, move_by: int):
        doc_id = self.__base_doc_id + move_by
        result = self.__db.get(doc_id=doc_id)
        return result

    """ Merge """

    def merge(
        self,
        theirs: Dict | ParamStore,
        merge_strategy: Optional[MergeStrategy] = MergeStrategy.OURS,
    ) -> None:
        if issubclass(type(theirs), ParamStore):
            theirs = theirs.to_dict()
        with self.__lock:
            ours = self.toDict()
            merged = self.__merge_trees(ours, theirs, merge_strategy)
            self.clear()
            self.update(merged)

    def __merge_trees(self, a: Dict, b: Dict, merge_strategy: MergeStrategy) -> Dict:
        """Merges dict b into dict a using the given strategy"""
        for key in b:
            if key in a:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    self.__merge_trees(a[key], b[key], merge_strategy)
                elif a[key] == b[key]:
                    pass  # same leaf value, nothing to do
                else:  # conflict:
                    if merge_strategy == MergeStrategy.OURS:
                        pass  # a takes precedence, ignore b
                    elif merge_strategy == MergeStrategy.THEIRS:
                        a[key] = b[key]  # b takes precedence, overwrite a
                        self.__is_dirty = True
                    else:
                        raise NotImplementedError(
                            f"MergeStrategy '{merge_strategy}' is not implemented"
                        )
            else:  # key from b is not in a:
                a[key] = b[key]  # "copy" from b to a
                self.__is_dirty = True
        return a

    def list_values(self, key: str) -> pd.DataFrame:
        with self.__lock:
            values = []
            commits = self.__db.all()
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
            if key in self.keys():
                values.append((self.__getitem__(key), None, None, None))
            df = pd.DataFrame(values)
            if not df.empty:
                df.columns = ["value", "time", "commit_id", "label"]
            return df

    """ Tags """

    def add_tag(self, tag: str, key: str) -> None:
        with self.__lock:
            if key not in self.keys():
                raise KeyError(f"key '{key}' is not in store")
            if tag not in self.__tags:
                self.__tags[tag] = []
            self.__tags[tag].append(key)
            self.__is_dirty = True

    def remove_tag(self, tag: str, key: str) -> None:
        with self.__lock:
            if tag not in self.__tags:
                return
            if key not in self.__tags[tag]:
                return
            self.__tags[tag].remove(key)
            self.__is_dirty = True

    def list_keys_for_tag(self, tag: str) -> List[str]:
        with self.__lock:
            if tag not in self.__tags:
                return []
            else:
                return self.__tags[tag]

    def list_tags_for_key(self, key: str):
        tags_for_key = []
        for item in self.__tags.items():
            if key in item[1]:
                tags_for_key.append(item[0])
        return tags_for_key

    """ Temporary State """

    def save_temp(self) -> None:
        """
        Saves the state of params to a temporary location
        """
        with self.__lock:
            table = self.__db.table("temp")
            doc = self.__build_document()
            table.upsert(Document(doc, doc_id=1))

    def load_temp(self) -> None:
        """
        Overwrites the current state of params with data loaded from the temporary
        location
        """
        with self.__lock:
            table = self.__db.table("temp")
            doc = table.get(doc_id=1)
            if not doc:
                raise EntropyError(
                    "Temp location is empty. Call save_temp() before calling load_temp()"
                )
            self.clear()
            self.update(doc["params"])
            self.__tags = doc["tags"]


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


def _ns_to_datetime(ns: int) -> pd.datetime:
    """Convert a UNIX epoch timestamp in nano-seconds to a human readable string"""
    return pd.to_datetime(ns)

from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import os.path
import shutil
import string
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from random import SystemRandom
from typing import Optional, Dict, Any, List, Callable, Set, MutableMapping

import jsonpickle as jsonpickle
import pandas as pd
from filelock import FileLock
from tinydb import TinyDB, Query
from tinydb.storages import MemoryStorage, Storage
from tinydb.table import Document
from tinydb.table import Table

from entropylab.logger import logger
from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.api.param_store import (
    ParamStore,
    MergeStrategy,
    Param,
    _ns_to_datetime,
)

CURRENT_VERSION = 0.2

INFO_DOC_ID = 1
INFO_TABLE = "info"
TEMP_DOC_ID = 1
TEMP_TABLE = "temp"
VERSION_KEY = "version"
REVISION_KEY = "revision"


class Metadata:
    def __init__(self, d: Dict = None):
        self.id: str = ""  # commit_id
        self.timestamp: int = time.time_ns()  # nanoseconds since epoch
        self.label: Optional[str] = None
        if d:
            self.__dict__.update(d)

    def __repr__(self) -> str:
        d = self.__dict__.copy()
        d["timestamp"] = _ns_to_datetime(self.timestamp)
        return f"<Metadata({_dict_to_json(d)})>"


class JSONPickleStorage(Storage):
    def __init__(self, filename):
        self.filename = filename

    def read(self):
        if not os.path.isfile(self.filename):
            return None
        with open(self.filename) as handle:
            # noinspection PyBroadException
            try:
                s = handle.read()
                data = jsonpickle.decode(s)
                return data
            except BaseException:
                logger.exception(
                    f"Exception decoding TinyDB JSON file '{self.filename}'"
                )
                return None

    def write(self, data):
        # noinspection PyBroadException
        try:
            with open(self.filename, "w+") as handle:
                s = jsonpickle.encode(data)
                handle.write(s)
        except BaseException:
            logger.exception(f"Exception encoding TinyDB JSON file '{self.filename}'")

    def close(self):
        pass


class InProcessParamStore(ParamStore):
    """Naive implementation of ParamStore based on tinydb

    Important:
    Using this implementation in multiple concurrent processes is not supported.
    """

    def __init__(
        self,
        path: Optional[str] | Optional[Path] = None,
        theirs: Optional[Dict | ParamStore] = None,
        merge_strategy: Optional[MergeStrategy] = MergeStrategy.THEIRS,
    ):

        super().__init__()

        self.__lock = threading.RLock()
        self.__base_commit_id: Optional[str] = None  # last commit checked out/committed
        self.__base_doc_id: Optional[int] = None  # tinydb document id of last commit...
        self.__params: Dict[str, Param] = dict()  # where current params are stored
        self.__tags: Dict[str, List[str]] = dict()  # tags that are mapped to keys
        self.__is_dirty: bool = False  # can the store be committed at this time?
        self.__dirty_keys: Set[str] = set()  # updated keys not committed yet

        if path is None:
            self.__is_in_memory_mode = True
            self.__db = TinyDB(storage=MemoryStorage)
            self.__filelock = contextlib.nullcontext()
            with self.__filelock:
                _set_version(self.__db, CURRENT_VERSION)
        else:
            self.__is_in_memory_mode = False
            path = str(path)
            is_new = not os.path.isfile(path)
            if not os.path.isfile(path):
                logger.debug(
                    f"Could not find a ParamStore JSON file at '{path}'. "
                    f"A new, empty, file will be created."
                )
                is_new = True
            self.__db = TinyDB(path, storage=JSONPickleStorage)
            Table.default_query_cache_capacity = 0
            self.__filelock = FileLock(path + ".lock")
            with self.__filelock:
                if is_new:
                    logger.debug(f"Creating new ParamStore JSON file at '{path}'")
                    _set_version(self.__db, CURRENT_VERSION)
                else:
                    self.checkout()
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
                self.__params.__setitem__(key, Param(value))
                self.__is_dirty = True
                self.__dirty_keys.add(key)

    def __getitem__(self, key: str) -> Any:
        with self.__lock:
            return self.__params.__getitem__(key).value

    def __delitem__(self, *args, **kwargs):
        with self.__lock:
            key = args[0]
            self.__params.__delitem__(*args, **kwargs)
            self.__remove_key_from_tags(key)
            self.__is_dirty = True
            self.__dirty_keys.add(key)

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            try:
                return self[key]
            except KeyError:
                raise AttributeError(key)

    def __setattr__(self, key, value):
        try:
            object.__getattribute__(self, key)
        except AttributeError:
            try:
                self[key] = value
            except BaseException:
                raise AttributeError(key)
        else:
            object.__setattr__(self, key, value)

    def __iter__(self):
        with self.__lock:
            values = _extract_param_values(self.__params)
            return values.__iter__()

    def __len__(self):
        with self.__lock:
            return self.__params.__len__()

    def __contains__(self, key):
        with self.__lock:
            return self.__params.__contains__(key)

    def __repr__(self):
        with self.__lock:
            return f"<InProcessParamStore({self.to_dict().__repr__()})>"

    def keys(self):
        with self.__lock:
            return self.__params.keys()

    def to_dict(self) -> Dict:
        with self.__lock:
            return _extract_param_values(self.__params)

    def get_value(self, key: str, commit_id: Optional[str] = None) -> object:
        with self.__lock:
            if commit_id is None:
                return self[key]
            else:
                commit = self.__get_commit(commit_id)
                return commit["params"][key].value

    def get_param(self, key: str, commit_id: Optional[str] = None) -> Param:
        with self.__lock:
            if commit_id is None:
                return copy.deepcopy(self.__params[key])
            else:
                commit = self.__get_commit(commit_id)
                return copy.deepcopy(commit["params"][key])

    def set_param(self, key: str, value: object, **kwargs):
        if "commit_id" in kwargs:
            raise ValueError("Setting commit_id in set_param() is not allowed")
        if "value" in kwargs:
            raise ValueError("Value can only be set through positional argument")
        with self.__lock:
            if key in self.__params:
                param = self.get_param(key)
            else:
                param = Param(value)
            param.value = value
            param.__dict__.update(kwargs)
            self.__params.__setitem__(key, param)
            self.__is_dirty = True
            self.__dirty_keys.add(key)

    def __remove_key_from_tags(self, key: str):
        for tag in self.__tags:
            if key in self.__tags[tag]:
                self.__tags[tag].remove(key)

    def rename_key(self, key: str, new_key: str):
        with self.__lock:
            if new_key in self.keys():
                raise KeyError(
                    f"Cannot rename key '{key}' to key that already exists: '{new_key}'"
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
            commit_id = self.__generate_commit_id()
            commit_timestamp = time.time_ns()  # nanoseconds since epoch
            self.__stamp_dirty_params_with_commit(commit_id, commit_timestamp)
            doc = self.__build_document(commit_id, label)
            with self.__filelock:
                doc.doc_id = self.__next_doc_id()
                doc_id = self.__db.insert(doc)
            self.__base_commit_id = doc["metadata"]["id"]
            self.__base_doc_id = doc_id
            self.__is_dirty = False
            self.__dirty_keys.clear()
            return doc["metadata"]["id"]

    def __next_doc_id(self):
        with self.__filelock:
            last_doc = self.__get_latest_commit()
            if last_doc:
                return last_doc.doc_id + 1
            else:
                return 1

    def __stamp_dirty_params_with_commit(self, commit_id: str, commit_timestamp: int):
        for key in self.__dirty_keys:
            if key in self.__params:
                param = self.__params[key]
                param.commit_id = commit_id
                if isinstance(param.expiration, timedelta):
                    expiration_in_ns = param.expiration.total_seconds() * 1e9
                    param.expiration = commit_timestamp + expiration_in_ns

    @staticmethod
    def __generate_commit_id():
        random_string = "".join(
            SystemRandom().choice(string.printable) for _ in range(32)
        ).encode("utf-8")
        return hashlib.sha1(random_string).hexdigest()

    def __build_document(
        self, commit_id: Optional[str] = None, label: Optional[str] = None
    ) -> Document:
        """
        builds a document to be saved as a commit/temp in TinyDB.
        :param commit_id: is None when saving to temp.
        :param label: an optional label to associate the commit with.
        :return: a dictionary describing the current state of the ParamStore
        """
        metadata = self.__build_metadata(commit_id, label)
        if self.__is_in_memory_mode:
            params = copy.deepcopy(self.__params)
        else:
            params = self.__params
        return Document(
            dict(metadata=metadata.__dict__, params=params, tags=self.__tags), doc_id=0
        )

    @staticmethod
    def __build_metadata(
        commit_id: Optional[str] = None, label: Optional[str] = None
    ) -> Metadata:
        metadata = Metadata()
        metadata.id = commit_id
        metadata.timestamp = time.time_ns()
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
            if commit:
                self.__checkout(commit)

    def __checkout(self, commit: Document):
        self.__params.clear()
        self.__params.update(commit["params"])
        self.__tags = commit["tags"]
        self.__base_commit_id = commit["metadata"]["id"]
        self.__base_doc_id = commit.doc_id
        self.__is_dirty = False
        self.__dirty_keys.clear()

    def list_commits(self, label: Optional[str] = None) -> List[Metadata]:
        with self.__lock:
            with self.__filelock:
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
    ) -> Optional[Document]:
        with self.__lock:
            if commit_id is not None:
                commit = self.__get_commit_by_id(commit_id)
            elif commit_num is not None:
                commit = self.__get_commit_by_num(commit_num)
            elif move_by is not None:
                commit = self.__get_commit_by_move_by(move_by)
            else:
                commit = self.__get_latest_commit()
            return commit

    def __get_commit_by_id(self, commit_id: str) -> Document:
        with self.__filelock:
            result = self.__db.search(Query().metadata.id == commit_id)
            if len(result) == 0:
                raise EntropyError(f"Commit with id '{commit_id}' not found")
            if len(result) > 1:
                raise EntropyError(
                    f"{len(result)} commits with id '{commit_id}' found. "
                    f"Only one commit is allowed per id"
                )
            return result[0]

    def __get_commit_by_num(self, commit_num: int) -> Document:
        with self.__filelock:
            result = self.__db.get(doc_id=commit_num)
            if result is None:
                raise EntropyError(f"Commit with number '{commit_num}' not found")
            return result

    def __get_commit_by_move_by(self, move_by: int) -> Document:
        doc_id = self.__base_doc_id + move_by
        with self.__filelock:
            result = self.__db.get(doc_id=doc_id)
            return result

    def __get_latest_commit(self) -> Optional[Document]:
        with self.__filelock:
            commits = self.__db.all()
            return commits[-1] if commits else None

    """ Merge """

    def merge(
        self,
        theirs: ParamStore,
        merge_strategy: Optional[MergeStrategy] = MergeStrategy.OURS,
    ) -> None:
        with self.__lock:
            ours = self
            self.__merge_trees(ours, theirs, merge_strategy)

    def __merge_trees(
        self,
        a: ParamStore | Dict,
        b: ParamStore | Dict,
        merge_strategy: MergeStrategy,
    ) -> bool:
        """Merges `b` into `a` *in-place* using the given strategy"""
        a_has_changed = False
        for key in b.keys():
            if key in a.keys():
                if (
                    (not isinstance(a[key], Param))
                    and isinstance(a[key], dict)
                    and (not isinstance(b[key], Param))
                    and isinstance(b[key], dict)
                ):
                    """This is a special case where the values of the Params are both
                    dictionaries. In this case we merge the dictionary from b into the
                    dictionary from a using the given strategy."""
                    a_has_changed = a_has_changed or self.__merge_trees(
                        a[key], b[key], merge_strategy
                    )
                    if (
                        a_has_changed
                        and isinstance(a, ParamStore)
                        and isinstance(b, ParamStore)
                    ):
                        """if the dictionary in a has been changed, this was done
                        in-place. We therefore need mark the Param key as dirty. We only
                        do this at the very top of the recursion - when a and b are the
                        ParamStores being merged"""
                        self.__is_dirty = True
                        self.__dirty_keys.add(key)
                elif a[key] == b[key]:
                    pass  # same leaf values, nothing to do
                else:  # diff leave values => conflict:
                    if merge_strategy == MergeStrategy.OURS:
                        pass  # a takes precedence, ignore b
                    elif merge_strategy == MergeStrategy.THEIRS:
                        a[key] = b[key]  # b takes precedence, overwrite a
                        a_has_changed = True
                    else:
                        raise NotImplementedError(
                            f"MergeStrategy '{merge_strategy}' is not implemented"
                        )
            else:  # key from b is not in a:
                a[key] = b[key]  # "copy" from b to a
                a_has_changed = True
        return a_has_changed

    """ Diff """

    def diff(
        self, old_commit_id: Optional[str] = None, new_commit_id: Optional[str] = None
    ) -> Dict[str, Dict]:
        with self.__lock:

            # get OLD params to diff:
            if old_commit_id:
                old_commit = self.__get_commit(old_commit_id)
            else:  # default to latest commit
                old_commit = self.__get_latest_commit()
            old_params = old_commit["params"] if old_commit else {}

            # get NEW params to diff:
            if new_commit_id:
                new_commit = self.__get_commit(new_commit_id)
                new_params = new_commit["params"] if new_commit else {}
            else:  # default to dirty params
                new_params = self.__params

            return self.__diff(old_params, new_params)

    def __diff(self, old: MutableMapping, new: MutableMapping) -> Dict[str, Dict]:
        diff = dict()
        for key in new.keys():
            if key in old.keys():
                old_value = old[key].value
                new_value = new[key].value
                if old_value != new_value:  # different values
                    diff[key] = dict(old_value=old_value, new_value=new_value)
            else:
                diff[key] = dict(new_value=new[key].value)  # added
        for key in old.keys():
            if key not in new.keys():
                diff[key] = dict(old_value=old[key].value)  # deleted
        return diff

    @staticmethod
    def __safe_get_value_from_params(params: Dict[str, Param], key: str) -> Optional:
        if key in params:
            return params[key].value
        else:
            return None

    def list_values(self, key: str) -> pd.DataFrame:
        with self.__lock:
            values = []
            with self.__filelock:
                commits = self.__db.all()
            commits.sort(key=lambda x: x["metadata"]["timestamp"])
            for commit in commits:
                try:
                    value = (
                        commit["params"][key].value,
                        _ns_to_datetime(commit["metadata"]["timestamp"]),
                        commit["metadata"]["id"],
                        commit["metadata"]["label"],
                    )
                    values.append(value)
                except KeyError:
                    pass
            if self.__is_dirty and key in self.__params.keys():
                values.append((self[key], None, None, None))
            df = pd.DataFrame(values)
            if not df.empty:
                df.columns = ["value", "time", "commit_id", "label"]
            return df

    """ Tags """

    def add_tag(self, tag: str, key: str) -> None:
        with self.__lock:
            if key not in self.__params.keys():
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
            with self.__filelock:
                table = self.__db.table(TEMP_TABLE)
                doc = self.__build_document()
                doc.doc_id = TEMP_DOC_ID
                table.upsert(doc)

    def load_temp(self) -> None:
        """
        Overwrites the current state of params with data loaded from the temporary
        location
        """
        with self.__lock:
            with self.__filelock:
                table = self.__db.table(TEMP_TABLE)
            doc = table.get(doc_id=1)
            if not doc:
                raise EntropyError(
                    "Temp is empty. Call save_temp() before calling load_temp()"
                )
            self.__params.clear()
            self.__params.update(doc["params"])
            self.__tags = doc["tags"]


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


def _map_dict(f, d: Dict) -> Dict:
    values_dict = dict()
    for item in d.items():
        k = item[0]
        v = item[1]
        if not isinstance(v, Param) and isinstance(v, dict):
            values_dict[k] = _map_dict(f, v)
        else:
            values_dict[k] = f(v)
    return values_dict


def _extract_param_values(d: Dict) -> Dict:
    return _map_dict(lambda x: x.value, d)


def _map_dict_in_place(f, d: Dict):
    for item in d.items():
        k = item[0]
        v = item[1]
        if isinstance(v, dict):
            _map_dict_in_place(f, v)
        else:
            d[k] = f(v)


def _extract_param_values_in_place(d: Dict):
    _map_dict_in_place(lambda x: x.value, d)


""" Migrations """


def fix_param_qualified_name(path: str | Path, revision: str):
    """
    Backup and fix the fully qualified names of Param in an InProcessParamStore JSON
    file.

    :param path: path to an existing JSON TinyDB file containing params.
    """
    if not os.path.isfile(path):
        return
    OLD_NAME = "entropylab.api.in_process_param_store.Param"
    NEW_NAME = "entropylab.pipeline.api.param_store.Param"
    path = str(path)
    backup_path = _backup_file(path, revision)
    # TODO: Support locking
    with TinyDB(backup_path) as old_db:
        old_commits = old_db.all()
        old_temp = old_db.table(TEMP_TABLE).get(doc_id=TEMP_DOC_ID)
        old_info = old_db.table(INFO_TABLE).get(doc_id=INFO_DOC_ID)
    with TinyDB(path) as new_db:
        for old_commit in old_commits:
            new_commit = copy.deepcopy(old_commit)
            for param in new_commit["params"].values():
                if "py/object" in param and param["py/object"] == OLD_NAME:
                    param["py/object"] = NEW_NAME
            new_db.insert(new_commit)
        if old_temp:
            new_temp = copy.deepcopy(old_temp)
            new_db.table(TEMP_TABLE).insert(new_temp)
        if old_info:
            new_info = copy.deepcopy(old_info)
            new_db.table(INFO_TABLE).insert(new_info)


def migrate_param_store_0_1_to_0_2(path: str | Path, revision: str) -> None:
    """
    Backup and migrate an InProcessParamStore JSON file from storing values to storing
    Params containing the values. Preserves commits, timestamps and ids.

    :param path: path to an existing JSON TinyDB file containing params.
    """
    old_version = "0.1"
    new_version = "0.2"

    path = str(path)
    _check_version(path, old_version, new_version)
    backup_path = _backup_file(path, revision)
    # TODO: Support locking
    with TinyDB(backup_path) as old_db:
        old_commits = old_db.all()
        old_temp = old_db.table(TEMP_TABLE).get(doc_id=TEMP_DOC_ID)
    with TinyDB(path, storage=JSONPickleStorage) as new_db:
        for old_commit in old_commits:
            new_commit = copy.deepcopy(old_commit)
            _wrap_params(new_commit)
            _rename_ns(new_commit)
            new_db.insert(new_commit)
        if old_temp:
            new_temp = copy.deepcopy(old_temp)
            _wrap_params(new_temp)
            _rename_ns(new_temp)
            new_db.table(TEMP_TABLE).insert(new_temp)


def _backup_file(path: str, revision: str):
    backup_path = f"{path}.{revision}.bak"
    shutil.move(path, backup_path)
    return backup_path


def _wrap_params(new_commit):
    params = new_commit["params"]
    for key in params.keys():
        value = params[key]
        if not isinstance(value, Param):
            params[key] = Param(value)
    new_commit["params"] = params


def _rename_ns(new_commit):
    timestamp = new_commit["metadata"]["ns"]
    del new_commit["metadata"]["ns"]
    new_commit["metadata"]["timestamp"] = timestamp


def _set_version(db: TinyDB | str, version: str, revision: Optional[str] = ""):
    if isinstance(db, str):
        # TODO: Support locking
        db = TinyDB(db, storage=JSONPickleStorage)
    info_table = db.table(INFO_TABLE)
    info_doc = info_table.get(doc_id=INFO_DOC_ID)
    if info_doc:
        info_doc[VERSION_KEY] = version
        info_doc[REVISION_KEY] = revision
    else:
        info_table.insert(dict(VERSION_KEY=version, REVISION_KEY=revision))


def _get_version(db: TinyDB):
    if INFO_TABLE in db.tables():
        info_table = db.table(INFO_TABLE)
        info_doc = info_table.get(doc_id=INFO_DOC_ID)
        return info_doc[VERSION_KEY]
    return "0.1"


def _check_version(path: str, old_version: str, new_version: str):
    if os.path.isfile(path):
        with TinyDB(path) as old_db:
            actual_version = _get_version(old_db)
            if actual_version != old_version:
                raise EntropyError(
                    f"Cannot migrate file '{path}' from version {actual_version} to "
                    f"version {new_version}"
                )
    else:
        raise EntropyError(
            f"Cannot migrate file '{path}' to version {new_version} because the file "
            "does not exist"
        )

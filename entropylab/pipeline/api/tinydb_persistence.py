from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import os
import string
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from random import SystemRandom
from typing import Any, Optional, Callable, List, Dict

import jsonpickle as jsonpickle
from filelock import FileLock
from tinydb import TinyDB, Query
from tinydb.storages import Storage, MemoryStorage
from tinydb.table import Table, Document

from entropylab.logger import logger
from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.api.param_store import _ns_to_datetime

CURRENT_VERSION = "0.2"

INFO_DOC_ID = 1
INFO_TABLE = "info"
TEMP_DOC_ID = 1
TEMP_TABLE = "temp"
VERSION_KEY = "version"
REVISION_KEY = "revision"


@dataclass
class Commit:
    label: Optional[str]
    params: Dict
    tags: Dict
    id: Optional[str] = None  # commit_id
    timestamp: Optional[int] = None  # nanoseconds since epoch

    def __post_init__(self):
        self.id = self.id or ""
        self.timestamp = self.timestamp or time.time_ns()
        self.label = self.label or None
        self.params = self.params or {}
        self.tags = self.tags or {}


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


class TinyDBPersistence:
    def __init__(self, path: Optional[str] | Optional[Path] = None):
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
            self.__db = TinyDB(path, storage=JSONPickleStorage)
            Table.default_query_cache_capacity = 0
            self.__filelock = FileLock(path + ".lock")
            with self.__filelock:
                if is_new:
                    logger.debug(f"Creating new ParamStore JSON file at '{path}'")
                    _set_version(self.__db, CURRENT_VERSION)
                else:
                    version = _get_version(self.__db)
                    if version != CURRENT_VERSION:
                        raise EntropyError(
                            f"ParamStore JSON file at '{path}' is version {version}. "
                            f"Please upgrade to {CURRENT_VERSION}."
                        )

    def close(self):
        with self.__filelock:
            self.__db.close()

    def get_commit(
        self,
        commit_id: Optional[str] = None,
        commit_num: Optional[int] = None,
        move_by: Optional[int] = None,
    ) -> Optional[Document]:
        if commit_id is not None:
            commit = self.get_commit_by_id(commit_id)
        elif commit_num is not None:
            commit = self.get_commit_by_num(commit_num)
        elif move_by is not None:
            commit = self.get_commit_by_move_by(move_by)
        else:
            commit = self.get_latest_commit()
        return commit

    def get_commit_by_id(self, commit_id: str) -> Any:
        with self.__filelock:
            return self.__db.search(Query().metadata.id == commit_id)

    def get_commit_by_num(self, commit_num: int) -> Any:
        with self.__filelock:
            result = self.__db.get(doc_id=commit_num)
            if result is None:
                raise EntropyError(f"Commit with number '{commit_num}' not found")
            return result

    def get_commit_by_move_by(self, current_pos: int, move_by: int) -> Any:
        doc_id = current_pos + move_by
        with self.__filelock:
            return self.__db.get(doc_id=doc_id)

    def get_latest_commit(self) -> Optional[Commit]:
        with self.__filelock:
            latest_doc = self.__get_latest_doc()
            if latest_doc:
                return self.__doc_to_commit(latest_doc)
            else:
                return None

    @staticmethod
    def __doc_to_commit(doc: Document) -> Commit:
        return Commit(
            id=doc["metadata"]["id"],
            timestamp=doc["metadata"]["timestamp"],
            label=doc["metadata"]["label"],
            params=doc["params"],
            tags=doc["tags"],
        )

    def __get_latest_doc(self) -> Optional[Document]:
        with self.__filelock:
            docs = self.__db.all()
            return docs[-1] if docs else None

    def commit(self, commit: Commit, label: Optional[str] = None) -> str:
        commit.id = self.generate_commit_id()
        commit.timestamp = time.time_ns()  # nanoseconds since epoch
        commit.label = label
        self.__stamp_dirty_params_with_commit(commit, commit.id, commit.timestamp)
        doc = self.__build_document(commit)
        with self.__filelock:
            doc.doc_id = self.__next_doc_id()
            doc_id = self.__db.insert(doc)
        return doc["metadata"]["id"]

    @staticmethod
    def generate_commit_id():
        random_string = "".join(
            SystemRandom().choice(string.printable) for _ in range(32)
        ).encode("utf-8")
        return hashlib.sha1(random_string).hexdigest()

    # TODO: How to lift this into ParamStore?
    def __stamp_dirty_params_with_commit(
        self, commit: Commit, commit_id: str, commit_timestamp: int
    ):
        # for key in self.__dirty_keys:
        #     if key in self.__params:
        #         param = self.__params[key]
        #         param.commit_id = commit_id
        #         if isinstance(param.expiration, timedelta):
        #             expiration_in_ns = param.expiration.total_seconds() * 1e9
        #             param.expiration = commit_timestamp + expiration_in_ns
        pass

    def __build_document(self, commit: Commit) -> Document:
        """
        builds a document to be saved as a commit/temp in TinyDB.
        :param commit:
        :return: a dictionary describing the current state of the ParamStore
        """
        metadata = self.__build_metadata(commit)
        if self.__is_in_memory_mode:
            params = copy.deepcopy(commit.params)
        else:
            params = commit.params
        return Document(
            dict(metadata=metadata.__dict__, params=params, tags=commit.tags),
            doc_id=0,
        )

    @staticmethod
    def __build_metadata(commit: Commit) -> Metadata:
        metadata = Metadata()
        metadata.id = commit.id
        metadata.timestamp = commit.timestamp
        metadata.label = commit.label
        return metadata

    def __next_doc_id(self):
        with self.__filelock:
            last_doc = self.__get_latest_doc()
            if last_doc:
                return last_doc.doc_id + 1
            else:
                return 1

    def checkout(self, commit: Document):
        self.__params.clear()
        self.__params.update(commit["params"])
        self.__tags = commit["tags"]
        self.__base_commit_id = commit["metadata"]["id"]
        self.__base_doc_id = commit.doc_id
        self.__is_dirty = False
        self.__dirty_keys.clear()

    # TODO: List[Commit] instead of List[Document]
    def search_commits(self, label: Optional[str] = None) -> List[Document]:
        def test_if_value_contains(lbl: str) -> Callable:
            return lambda val: (lbl or "") in (val or "")

        with self.__filelock:
            return self.__db.search(
                Query().metadata.label.test(test_if_value_contains(label))
            )

    def set_temp_commit(self):
        pass

    def get_temp_commit(self):
        pass


def _dict_to_json(d: Dict) -> str:
    return json.dumps(d, default=_json_dumps_default, sort_keys=True, ensure_ascii=True)


def _json_dumps_default(value):
    if isinstance(value, datetime):
        return str(value)
    else:
        return value.__dict__


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
        info_table.insert({VERSION_KEY: version, REVISION_KEY: revision})


def _get_version(db: TinyDB):
    if INFO_TABLE in db.tables():
        info_table = db.table(INFO_TABLE)
        info_doc = info_table.get(doc_id=INFO_DOC_ID)
        return info_doc[VERSION_KEY] if VERSION_KEY in info_doc else None
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

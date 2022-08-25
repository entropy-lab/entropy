from __future__ import annotations

import contextlib
import copy
import hashlib
import os
import string
from pathlib import Path
from random import SystemRandom
from typing import Optional, Callable, List, Set

from filelock import FileLock
from tinydb import TinyDB, Query
from tinydb.storages import MemoryStorage
from tinydb.table import Table, Document

from entropylab.logger import logger
from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.params.persistence.persistence import (
    Persistence,
    Commit,
)
from entropylab.pipeline.params.persistence.tinydb.storage import JSONPickleStorage

CURRENT_VERSION = "0.2"

INFO_DOC_ID = 1
INFO_TABLE = "info"
TEMP_DOC_ID = 1
TEMP_TABLE = "temp"
VERSION_KEY = "version"
REVISION_KEY = "revision"


class TinyDbPersistence(Persistence):
    def __init__(self, path: Optional[str] | Optional[Path] = None):
        if path is None:
            self.__is_in_memory_mode = True
            self.__db = TinyDB(storage=MemoryStorage)
            self.__filelock = contextlib.nullcontext()
            with self.__filelock:
                set_version(self.__db, CURRENT_VERSION)
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
                    set_version(self.__db, CURRENT_VERSION)
                else:
                    version = get_version(self.__db)
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
    ) -> Optional[Commit]:
        if commit_id is not None:
            doc = self.__get_commit_by_id(commit_id)
        elif commit_num is not None:
            doc = self.__get_commit_by_num(commit_num)
        else:
            doc = self.__get_latest_doc()
        return self.__doc_to_commit(doc)

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

    def get_latest_commit(self) -> Optional[Commit]:
        with self.__filelock:
            latest_doc = self.__get_latest_doc()
            if latest_doc:
                return self.__doc_to_commit(latest_doc)
            else:
                return None

    @staticmethod
    def __doc_to_commit(doc: Optional[Document]) -> Optional[Commit]:
        if not doc:
            return None
        else:
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

    def commit(
        self,
        commit: Commit,
        dirty_keys: Optional[Set[str]] = None,
    ) -> str:
        commit.id = self.__generate_commit_id()
        # TODO: Perhaps create the timestamp here?
        self.stamp_dirty_params_with_commit(commit, dirty_keys)
        doc = self.__build_document(commit)
        with self.__filelock:
            doc.doc_id = self.__next_doc_id()
            self.__db.insert(doc)
        return commit.id

    @staticmethod
    def __generate_commit_id() -> str:
        random_string = "".join(
            SystemRandom().choice(string.printable) for _ in range(32)
        ).encode("utf-8")
        return hashlib.sha1(random_string).hexdigest()

    def __build_document(self, commit: Commit) -> Document:
        """
        builds a document to be saved as a commit/temp in TinyDB.
        :param commit:
        :return: a dictionary describing the current state of the ParamStore
        """
        metadata = commit.to_metadata()
        if self.__is_in_memory_mode:
            params = copy.deepcopy(commit.params)
        else:
            params = commit.params
        return Document(
            dict(metadata=metadata.__dict__, params=params, tags=commit.tags),
            doc_id=0,
        )

    def __next_doc_id(self):
        with self.__filelock:
            last_doc = self.__get_latest_doc()
            if last_doc:
                return last_doc.doc_id + 1
            else:
                return 1

    def search_commits(
        self, label: Optional[str] = None, key: Optional[str] = None
    ) -> List[Commit]:
        def test_label(lbl: str) -> Callable:
            return lambda metadata_label: metadata_label == lbl if lbl else True

        def test_key(k: str) -> Callable:
            return lambda params: k in params if k else True

        with self.__filelock:
            docs = self.__db.search(
                Query().metadata.label.test(test_label(label))
                & Query().params.test(test_key(key))
            )
            return list(map(self.__doc_to_commit, docs))

    # noinspection PyShadowingNames
    def save_temp_commit(self, commit: Commit) -> None:
        with self.__filelock:
            table = self.__db.table(TEMP_TABLE)
            doc = self.__build_document(commit)
            doc.doc_id = TEMP_DOC_ID
            table.upsert(doc)

    """ Temporary State """

    def load_temp_commit(self) -> Commit:
        with self.__filelock:
            table = self.__db.table(TEMP_TABLE)
        doc = table.get(doc_id=1)
        if not doc:
            raise EntropyError(
                "Temp is empty. Use save_temp_commit() before using load_temp_commit()"
            )
        else:
            return self.__doc_to_commit(doc)


def set_version(db: TinyDB | str, version: str, revision: Optional[str] = ""):
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


def get_version(db: TinyDB):
    if INFO_TABLE in db.tables():
        info_table = db.table(INFO_TABLE)
        info_doc = info_table.get(doc_id=INFO_DOC_ID)
        return info_doc[VERSION_KEY] if VERSION_KEY in info_doc else None
    return "0.1"


def check_version(path: str, old_version: str, new_version: str):
    if os.path.isfile(path):
        with TinyDB(path) as old_db:
            actual_version = get_version(old_db)
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

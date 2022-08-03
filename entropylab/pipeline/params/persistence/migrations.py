from __future__ import annotations

import copy
import os
import shutil
from pathlib import Path

import pandas as pd
from tinydb import TinyDB

from entropylab.pipeline.params.param_store import Param
from entropylab.pipeline.params.persistence.tinydb.storage import JSONPickleStorage
from entropylab.pipeline.params.persistence.tinydb.tinydbpersistence import (
    _check_version,
    TEMP_TABLE,
    TEMP_DOC_ID,
    INFO_TABLE,
    INFO_DOC_ID,
)


def fix_param_qualified_name(path: str | Path, revision: str):
    __fix_param_qualified_name(
        path,
        revision,
        "entropylab.api.in_process_param_store.Param",
        "entropylab.pipeline.api.param_store.Param",
    )


def __fix_param_qualified_name(
    path: str | Path, revision: str, old_name: str, new_name: str
):
    """
    Backup and fix the fully qualified names of Param in an ParamStore JSON
    file.

    :param path: path to an existing JSON TinyDB file containing params.
    :param revision:
    :param old_name: The qualified name in the given ParamStore JSON
    :param new_name: The new qualified name
    """
    if not os.path.isfile(path):
        return
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
                if "py/object" in param and param["py/object"] == old_name:
                    param["py/object"] = new_name
            new_db.insert(new_commit)
        if old_temp:
            new_temp = copy.deepcopy(old_temp)
            new_db.table(TEMP_TABLE).insert(new_temp)
        if old_info:
            new_info = copy.deepcopy(old_info)
            new_db.table(INFO_TABLE).insert(new_info)


def migrate_param_store_0_1_to_0_2(path: str | Path, revision: str) -> None:
    """
    Backup and migrate an ParamStore JSON file from storing values to storing
    Params containing the values. Preserves commits, timestamps and ids.

    :param path: path to an existing JSON TinyDB file containing params.
    :param revision: the Alembic revision (version) that calls this function

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


def migrate_param_store_0_2_to_0_3(path: str | Path, revision: str) -> None:
    """
    Backup and migrate an ParamStore JSON file to use the newer qualified name for
    Param instances and convert `commit.timestamp` values from int to pd.Timestamp

    :param path: path to an existing JSON TinyDB file containing params.
    :param revision: the Alembic revision (version) that calls this function

    """
    old_version = "0.2"
    new_version = "0.3"

    path = str(path)
    _check_version(path, old_version, new_version)
    __fix_param_qualified_name(
        path,
        revision,
        "entropylab.pipeline.api.param_store.Param",
        "entropylab.pipeline.params.param_store.Param",
    )

    backup_path = _backup_path(path, revision)

    # TODO: Support locking
    with TinyDB(backup_path) as old_db:
        old_commits = old_db.all()
        old_temp = old_db.table(TEMP_TABLE).get(doc_id=TEMP_DOC_ID)
    with TinyDB(path, storage=JSONPickleStorage) as new_db:
        for old_commit in old_commits:
            new_commit = copy.deepcopy(old_commit)
            new_commit.timestamp = pd.Timestamp(old_commit.timestamp)
            new_db.insert(new_commit)
        if old_temp:
            new_temp = copy.deepcopy(old_temp)
            new_temp.timestamp = pd.Timestamp(old_temp.timestamp)
            new_db.table(TEMP_TABLE).insert(new_temp)


def _backup_path(path: str, revision: str):
    return f"{path}.{revision}.bak"


def _backup_file(path: str, revision: str):
    backup_path = _backup_path(path, revision)
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

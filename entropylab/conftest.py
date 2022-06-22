import multiprocessing
import os
import shutil
import traceback
from datetime import datetime
from pathlib import Path

import pytest

from entropylab.logger import logger
from entropylab.pipeline.results_backend.sqlalchemy.db_initializer import (
    _ENTROPY_DIRNAME,
    _DB_FILENAME,
)

"""conftest.py is a standard pytest configuration file (see here:
https://docs.pytest.org/en/6.2.x/fixture.html).

This conftest.py file contains pytest test fixtures that generate project-related test
data and, on test tear-down, remove the data from disk.
"""


class Process(multiprocessing.Process):
    """
    Class which returns child Exceptions to Parent.
    https://stackoverflow.com/a/33599967/4992248
    """

    def __init__(self, *args, **kwargs):
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._parent_conn, self._child_conn = multiprocessing.Pipe()
        self._exception = None

    def run(self):
        try:
            multiprocessing.Process.run(self)
            self._child_conn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._child_conn.send((e, tb))
            # raise e  # You can still rise this exception if you need to

    @property
    def exception(self):
        if self._parent_conn.poll():
            self._exception = self._parent_conn.recv()
        return self._exception


# Test fixtures


@pytest.fixture()
def project_dir_path(request) -> str:
    """Generate a unique path to a (non-existent) project directory
    The directory will be removed recursively on tear down"""
    dir_path = _build_project_dir_path_for_test(request)
    yield dir_path
    _delete_if_exists(dir_path)


@pytest.fixture()
def db_file_path(request) -> str:
    """Generate a unique path to a (non-existent) sqlite db file
    The file will be removed on tear down"""
    dir_path = _build_project_dir_path_for_test(request)
    file_path = dir_path + ".db"
    yield file_path
    _delete_if_exists(file_path)
    _delete_if_exists(dir_path)


@pytest.fixture()
def tinydb_file_path(request) -> str:
    """Generate a unique path to a (non-existent) tinydb json file
    The file will be removed on tear down"""
    dir_path = _build_project_dir_path_for_test(request)
    file_path = os.path.join(dir_path + "/" + "tiny_db.json")
    yield file_path
    _delete_if_exists(dir_path)


@pytest.fixture()
def initialized_project_dir_path(request, project_dir_path) -> str:
    """Create an initialized project and return the path to its directory
    The directory will be removed recursively on tear down"""
    entropy_dir = os.path.join(project_dir_path, _ENTROPY_DIRNAME)
    os.makedirs(entropy_dir)
    # if used in a parametrized test, consider the parameter as a db_template
    # and use a copy of it as the db:
    if hasattr(request, "param") and request.param is not None:
        # param input from test annotation is the db_template file name:
        db_template = os.path.join("./db_templates", str(request.param))
        db_file = os.path.join(entropy_dir, _DB_FILENAME)
        _copy_template(db_template, db_file, request)
    return project_dir_path


# Internal implementations


def _build_project_dir_path_for_test(request):
    project_dir_path = (
        f"tests_cache/{request.node.name}_{datetime.now():%Y-%m-%d-%H-%M-%S}"
    )
    if os.path.isdir(project_dir_path):
        logger.info(f"Test cleanup. Deleting directory '{project_dir_path}'")
        shutil.rmtree(project_dir_path)
    Path(project_dir_path).mkdir(parents=True, exist_ok=True)
    return project_dir_path


def _delete_if_exists(path: str):
    if os.path.isdir(path):
        logger.debug(f"Test cleanup. Deleting directory '{path}'")
        shutil.rmtree(path)
    elif os.path.isfile(path):
        logger.debug(f"Test cleanup. Deleting file '{path}'")
        os.remove(path)


def _copy_template(src, dst, request):
    """Copy the source template file (path relative to test file) to the destination"""
    if src is not None and src != "":
        abs_src = os.path.join(request.fspath.dirname, src)
        logger.debug(f"Copied db template from '{abs_src}' to '{dst}'")
        shutil.copyfile(abs_src, dst)

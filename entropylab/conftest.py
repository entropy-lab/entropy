import os
import shutil
from datetime import datetime

import pytest

from entropylab.logger import logger
from entropylab.results_backend.sqlalchemy.db_initializer import (
    _ENTROPY_DIRNAME,
    _DB_FILENAME,
)

"""conftest.py is a standard pytest configuration file (see here:
https://docs.pytest.org/en/6.2.x/fixture.html).

This conftest.py file contains pytest test fixtures that generate project-related test
data and, on test tear-down, remove the data from disk.
"""


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
    file_path = _build_project_dir_path_for_test(request) + ".db"
    yield file_path
    _delete_if_exists(file_path)


@pytest.fixture()
def initialized_project_dir_path(request, project_dir_path) -> str:
    """Create an initialized project and return the path to its directory
    The directory will be removed recursively on tear down"""
    entropy_dir = os.path.join(project_dir_path, _ENTROPY_DIRNAME)
    os.makedirs(entropy_dir)
    if hasattr(request, "param") and request.param is not None:
        db_template = str(request.param)  # param input from test annotation
        db_file = os.path.join(entropy_dir, _DB_FILENAME)
        _copy_db_template(db_template, db_file, request)
    return project_dir_path


# Internal implementations


def _build_project_dir_path_for_test(request):
    project_dir_path = (
        f"tests_cache/{request.node.name}_{datetime.now():%Y-%m-%d-%H-%M-%S}"
    )
    return project_dir_path


def _delete_if_exists(directory: str):
    if os.path.isdir(directory):
        logger.debug(f"Deleting test project directory '{directory}'")
        shutil.rmtree(directory)


def _copy_db_template(src, dst, request):
    """Copy the source DB (path relative to test file) to the destination dir"""
    if src is not None and src != "":
        abs_src = os.path.join(request.fspath.dirname, src)
        logger.debug(f"Copied db template from '{abs_src}' to '{dst}'")
        shutil.copyfile(abs_src, dst)

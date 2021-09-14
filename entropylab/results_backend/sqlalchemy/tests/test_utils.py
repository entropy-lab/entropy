import os
import shutil
from datetime import datetime
from entropylab.results_backend.sqlalchemy.db_initializer import (
    _ENTROPY_DIRNAME,
    _DB_FILENAME,
)


def build_project_dir_path_for_test(request):
    project_dir_path = (
        f"tests_cache/{request.node.name}_{datetime.now():%Y-%m-%d-%H-%M-%S}"
    )
    return project_dir_path


def create_test_project(request, db_template=None):
    project_dir = build_project_dir_path_for_test(request)
    entropy_dir = os.path.join(project_dir, _ENTROPY_DIRNAME)
    os.makedirs(entropy_dir)
    db_file = os.path.join(entropy_dir, _DB_FILENAME)
    _copy_db(db_template, db_file, request)
    return project_dir


def delete_if_exists(directory: str):
    if os.path.isdir(directory):
        shutil.rmtree(directory)


def _copy_db(src, dst, request):
    """ Copy the source DB (path relative to test file) to the destination dir """
    if src is not None and src != "":
        shutil.copyfile(os.path.join(request.fspath.dirname, src), dst)

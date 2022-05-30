from pathlib import Path

""" Project related functions.

An Entropy *project* is a filesystem directory containing a '.entropy' directory
which itself contains an 'entropy.db' SQLite database file and an 'entropy.hdf5'
HDF5 file.

project
    .entropy
        entropy.db
        entropy.hdf5
        params.json
"""


def project_name(path: str) -> str:
    """Returns a project name for the given path

    :path: A filesystem path to a directory. Directory does not have to contain an
    actual Entropy project"""
    return Path(path).absolute().name


def project_path(path: str) -> Path:
    """Returns the absolute path for the given path, as a string

    :path: A filesystem path to a directory. Directory does not have to contain an
    actual Entropy project"""
    return Path(path).absolute()


def db_file_path(prj_path: str) -> Path:
    return project_path(prj_path).joinpath(".entropy", "entropy.db")


def hdf5_dir_path(prj_path: str) -> Path:
    return project_path(prj_path).joinpath(".entropy", "hdf5")


def param_store_file_path(prj_path: str) -> Path:
    return project_path(prj_path).joinpath(".entropy", "params.json")


def dashboard_log_path(prj_path: str) -> Path:
    return project_path(prj_path).joinpath(".entropy", "dashboard.log")

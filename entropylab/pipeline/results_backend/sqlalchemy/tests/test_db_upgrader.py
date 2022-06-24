import os
import shutil

import pytest
from sqlalchemy import create_engine

from entropylab.pipeline import SqlAlchemyDB, RawResultData
from entropylab.conftest import _copy_template
from entropylab.logger import logger
from entropylab.pipeline.api.data_writer import Metadata
from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.api.in_process_param_store import InProcessParamStore
from entropylab.pipeline.results_backend.sqlalchemy.db_initializer import (
    _ENTROPY_DIRNAME,
    _DB_FILENAME,
    _HDF5_DIRNAME,
    _DbUpgrader,
)
from entropylab.pipeline.results_backend.sqlalchemy.project import param_store_file_path
from entropylab.pipeline.results_backend.sqlalchemy.storage import HDF5Storage


def test_upgrade_db_when_path_to_project_does_not_exist():
    # arrange
    target = _DbUpgrader("foo")
    # act & assert
    with pytest.raises(EntropyError):
        target.upgrade_db()


def test_upgrade_db_when_path_to_db_file_does_not_exist(db_file_path):
    # arrange
    target = _DbUpgrader(db_file_path)
    # act & assert
    with pytest.raises(EntropyError):
        target.upgrade_db()


def test_upgrade_db_assert_db_file_is_converted_to_project_dir(
    request, project_dir_path
):
    # arrange
    test_db_file_path = project_dir_path + ".db"
    db_template = (
        "empty_after_2021-08-01-14-18-43_04ae19b32c08_add_col_saved_in_hdf5.db"
    )
    db_template_path = os.path.join(
        request.fspath.dirname, "./db_templates/", db_template
    )
    shutil.copyfile(db_template_path, test_db_file_path)
    try:
        target = _DbUpgrader(test_db_file_path)
        # act
        target.upgrade_db()
        # assert
        assert not os.path.exists(test_db_file_path)
        assert os.path.exists(os.path.join(project_dir_path, ".entropy", "entropy.db"))
    finally:
        # clean up
        if os.path.isdir(test_db_file_path):
            logger.debug(f"Deleting test project directory '{test_db_file_path}'")
            shutil.rmtree(test_db_file_path)


@pytest.mark.parametrize(
    "initialized_project_dir_path",
    ["empty_after_2021-08-01-13-45-43_1318a586f31d_initial_migration.db"],
    indirect=True,
)
def test_upgrade_db_when_initial_db_is_empty(initialized_project_dir_path):
    # arrange
    target = _DbUpgrader(initialized_project_dir_path)
    # act
    target.upgrade_db()
    # assert
    engine = create_engine(
        f"sqlite:///{initialized_project_dir_path}/{_ENTROPY_DIRNAME}/{_DB_FILENAME}"
    )
    cur = engine.execute("SELECT sql FROM sqlite_master WHERE name = 'Results'")
    res = cur.fetchone()
    cur.close()
    assert "saved_in_hdf5" in res[0]


def test_upgrade_db_when_db_is_in_memory():
    # arrange
    target = _DbUpgrader(":memory:")
    # act
    target.upgrade_db()
    # assert
    cur = target._engine.execute("SELECT sql FROM sqlite_master WHERE name = 'Results'")
    res = cur.fetchone()
    cur.close()
    assert "saved_in_hdf5" in res[0]


def test__migrate_results_to_hdf5(initialized_project_dir_path):
    # arrange
    # save to DB but not to storage:
    db = SqlAlchemyDB(initialized_project_dir_path, enable_hdf5_storage=False)
    db.save_result(1, RawResultData(stage=1, label="foo", data="bar"))
    db.save_result(1, RawResultData(stage=1, label="baz", data="buz"))
    db.save_result(1, RawResultData(stage=2, label="biz", data="bez"))
    db.save_result(2, RawResultData(stage=1, label="bat", data="bot"))
    db.save_result(3, RawResultData(stage=1, label="ooh", data="aah"))
    target = _DbUpgrader(initialized_project_dir_path)
    # act
    target.upgrade_db()
    # assert
    storage = HDF5Storage(
        os.path.join(initialized_project_dir_path, _ENTROPY_DIRNAME, _HDF5_DIRNAME)
    )
    hdf5_results = storage.get_result_records()
    assert len(list(hdf5_results)) == 5
    cur = target._engine.execute("SELECT * FROM Results WHERE saved_in_hdf5 = 1")
    res = cur.all()
    assert len(res) == 5


def test__migrate_metadata_to_hdf5(initialized_project_dir_path):
    # arrange
    # save to DB but not to storage:
    db = SqlAlchemyDB(initialized_project_dir_path, enable_hdf5_storage=False)
    db.save_metadata(1, Metadata(stage=1, label="foo", data="bar"))
    db.save_metadata(1, Metadata(stage=1, label="baz", data="buz"))
    db.save_metadata(1, Metadata(stage=2, label="biz", data="bez"))
    db.save_metadata(2, Metadata(stage=1, label="bat", data="bot"))
    db.save_metadata(3, Metadata(stage=1, label="ooh", data="aah"))
    target = _DbUpgrader(initialized_project_dir_path)
    # act
    target.upgrade_db()
    # assert
    storage = HDF5Storage(
        os.path.join(initialized_project_dir_path, _ENTROPY_DIRNAME, _HDF5_DIRNAME)
    )
    hdf5_metadata = storage.get_metadata_records()
    assert len(list(hdf5_metadata)) == 5
    cur = target._engine.execute(
        "SELECT * FROM ExperimentMetadata WHERE saved_in_hdf5 = 1"
    )
    res = cur.all()
    assert len(res) == 5


@pytest.mark.parametrize(
    "initialized_project_dir_path",
    [
        "empty.db",
        "empty_after_2021-08-01-13-45-43_1318a586f31d_initial_migration.db",
        "empty_after_2021-08-01-14-18-43_04ae19b32c08_add_col_saved_in_hdf5.db",
        "empty_after_2022-03-17-15-57-28_f1ada2484fe2_create_figures_table.db",
        "empty_after_2022-04-10-08-26-35_9ffd2ba0d5bf_simplifying_node_id.db",
        "empty_after_2022-05-19-09-12-01_06140c96c8c4_wrapping_param_store_values.db",
    ],
    indirect=True,
)
def test_upgrade_db_from_all_revisions(initialized_project_dir_path):
    # arrange
    target = _DbUpgrader(initialized_project_dir_path)
    # act
    target.upgrade_db()
    # assert - all good if we didn't throw


@pytest.mark.parametrize(
    "initialized_project_dir_path",
    [
        "empty_after_2022-04-10-08-26-35_9ffd2ba0d5bf_simplifying_node_id.db",
    ],
    indirect=True,
)
def test_upgrade_db_upgrades_params_json_from_0_1_to_0_2(
    request, initialized_project_dir_path
):
    # arrange
    tinydb_file_path = str(param_store_file_path(initialized_project_dir_path))
    _copy_template(
        os.path.join("./db_templates", "param_store_v0_1.json"),
        tinydb_file_path,
        request,
    )
    target = _DbUpgrader(initialized_project_dir_path)
    # act
    target.upgrade_db()
    # assert - read a checked out value from param store as sanity:
    param_store = InProcessParamStore(tinydb_file_path)
    param_store.checkout("57ea4b9fb96bdc7a13fe8ec616a3c6da21f41ca0")
    assert param_store["qubit1.flux_capacitor"]["wave"] == "manifold"


@pytest.mark.parametrize(
    "initialized_project_dir_path",
    [
        "with_result_and_metadata_records_saved_in_hdf5.db",
    ],
    indirect=True,
)
def test_upgrade_db_deletes_results_and_metadata_from_sqlite(
    initialized_project_dir_path,
):
    # arrange
    target = _DbUpgrader(initialized_project_dir_path)
    # act
    target.upgrade_db()
    # assert for results
    cur = target._engine.execute("SELECT * FROM Results WHERE saved_in_hdf5 = 1")
    res = cur.all()
    assert len(res) == 0
    # assert for metadata
    cur = target._engine.execute(
        "SELECT * FROM ExperimentMetadata WHERE saved_in_hdf5 = 1"
    )
    res = cur.all()
    assert len(res) == 0

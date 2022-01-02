import pytest

from entropylab import SqlAlchemyDB, RawResultData


def test_save_result_raises_when_same_result_saved_twice(initialized_project_dir_path):
    # arrange
    db = SqlAlchemyDB(initialized_project_dir_path, enable_hdf5_storage=True)
    raw_result = RawResultData(stage=1, label="foo", data=42)
    db.save_result(0, raw_result)
    with pytest.raises(ValueError):
        # act & assert
        db.save_result(0, raw_result)


def test_get_last_result_of_experiment_works_with_storage(initialized_project_dir_path):
    # arrange
    db = SqlAlchemyDB(initialized_project_dir_path, enable_hdf5_storage=False)
    db.save_result(1, RawResultData(label="save", data="to db"))
    db = SqlAlchemyDB(initialized_project_dir_path, enable_hdf5_storage=True)
    db.save_result(1, RawResultData(label="save", data="to storage"))
    # act
    actual = db.get_last_result_of_experiment(1)
    # assert
    assert actual.data == "to storage"


def test_get_last_result_of_experiment_works_with_db(initialized_project_dir_path):
    # arrange
    db = SqlAlchemyDB(initialized_project_dir_path, enable_hdf5_storage=True)
    db.save_result(1, RawResultData(label="save", data="to storage"))
    db = SqlAlchemyDB(initialized_project_dir_path, enable_hdf5_storage=False)
    db.save_result(1, RawResultData(label="save", data="to db"))
    # act
    actual = db.get_last_result_of_experiment(1)
    # assert
    assert actual.data == "to db"

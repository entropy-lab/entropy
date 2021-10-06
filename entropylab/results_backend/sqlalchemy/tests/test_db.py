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

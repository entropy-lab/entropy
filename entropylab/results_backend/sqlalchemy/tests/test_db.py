import pytest

from entropylab import SqlAlchemyDB, RawResultData
from entropylab.results_backend.sqlalchemy.tests.test_utils import (
    delete_if_exists,
    create_test_project,
)


def test_save_result_raises_when_same_result_saved_twice(request):
    # arrange
    test_project_dir = create_test_project(request)
    try:
        db = SqlAlchemyDB(test_project_dir, echo=True, enable_hdf5_storage=True)
        raw_result = RawResultData(stage=1, label="foo", data=42)
        db.save_result(0, raw_result)
        with pytest.raises(ValueError):
            # act & assert
            db.save_result(0, raw_result)
    finally:
        # clean up
        delete_if_exists(test_project_dir)

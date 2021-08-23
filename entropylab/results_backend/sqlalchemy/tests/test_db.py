import os

import pytest

from config import settings
from entropylab import SqlAlchemyDB, RawResultData


HDF_FILENAME = "./entropy.hdf5"


def test_save_result_raises_when_same_result_saved_twice(request):
    # arrange
    settings.toggles = {"hdf5_storage": True}  # this feature is new in HDF5Storage
    path = f"./tests_cache/{request.node.name}.db"
    try:
        db = SqlAlchemyDB(path, echo=True)
        raw_result = RawResultData(stage=1, label="foo", data=42)
        db.save_result(0, raw_result)
        with pytest.raises(ValueError):
            # act & assert
            db.save_result(0, raw_result)
    finally:
        # clean up
        _delete_if_exists(HDF_FILENAME)
        _delete_if_exists(path)


def _delete_if_exists(filename: str):
    if os.path.isfile(filename):
        os.remove(filename)

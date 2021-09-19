import os
from pathlib import Path

import pytest

from entropylab import SqlAlchemyDB, RawResultData


def test_save_result_raises_when_same_result_saved_twice(request):
    # arrange
    path = f"./tests_cache/{request.node.name}.db"
    hdf5_path = str(Path(path).with_suffix(".hdf5"))
    try:
        db = SqlAlchemyDB(path, echo=True, enable_hdf5_storage=True)
        raw_result = RawResultData(stage=1, label="foo", data=42)
        db.save_result(0, raw_result)
        with pytest.raises(ValueError):
            # act & assert
            db.save_result(0, raw_result)
    finally:
        # clean up
        _delete_if_exists(hdf5_path)
        _delete_if_exists(path)


def _delete_if_exists(filename: str):
    if os.path.isfile(filename):
        os.remove(filename)

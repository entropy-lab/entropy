import datetime
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from entropylab import SqlAlchemyDB, RawResultData
from entropylab.results_backend.hdf5.results_db import HDF_FILENAME, ResultsDB
from entropylab.results_backend.sqlalchemy.model import ResultTable, ResultDataType


def test_get_results(request):
    # arrange
    db = f"./{request.node.name}.db"
    try:
        target = SqlAlchemyDB(db)
        result1 = __create_result(1, stage=0, label="foo", saved_in_hdf5=False)  # not migrated yet
        result2 = __create_result(2, stage=0, label="foo", saved_in_hdf5=False)  # not in experiment 1
        result3 = __create_result(1, stage=0, label="bar", saved_in_hdf5=True)  # already migrated
        target._execute_transaction(result1)
        target._execute_transaction(result2)
        target._execute_transaction(result3)
        ResultsDB().save_result(1, __create_raw_result_data(0, "bar"))
        # act
        results = target.get_results(1)
        # assert
        assert len(list(results)) == 2
    finally:
        # clean up
        if Path(db).exists(): os.remove(db, )
        if Path(HDF_FILENAME).exists(): os.remove(HDF_FILENAME)


def test_mark_results_as_migrated(request):
    # arrange
    db = f"./{request.node.name}.db"
    try:
        target = SqlAlchemyDB(db)
        target._execute_transaction(__create_result())
        target._execute_transaction(__create_result())
        target._execute_transaction(__create_result())
        # act
        target._SqlAlchemyDB__mark_results_as_migrated([1, 3])
        # assert
        session_maker = sessionmaker(create_engine(f"sqlite:///{db}"))
        with session_maker() as session:
            result1 = session.query(ResultTable).filter(ResultTable.id == 1).first()
            assert result1.saved_in_hdf5
            result2 = session.query(ResultTable).filter(ResultTable.id == 2).first()
            assert not result2.saved_in_hdf5
            result3 = session.query(ResultTable).filter(ResultTable.id == 3).first()
            assert result3.saved_in_hdf5
    finally:
        # clean up
        os.remove(db)


def __create_result(experiment_id=0, stage=0, label="foo", saved_in_hdf5=False):
    return ResultTable(
        experiment_id=experiment_id,
        stage=stage,
        story="",
        label=label,
        time=datetime.datetime.now(),
        data="bar".encode("UTF-8"),
        data_type=ResultDataType.String,
        saved_in_hdf5=saved_in_hdf5
    )


def __create_raw_result_data(stage=0, label="foo"):
    return RawResultData(
        label=label,
        data="bar".encode("UTF-8"),
        stage=stage,
        story="",
    )

# def __to_raw_result_data(result_table: ResultTable):
#     return RawResultData(
#         label=result_table.label,
#         data=result_table.data,
#         stage=result_table.stage,
#         story=result_table.story
#     )

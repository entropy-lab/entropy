import os
from datetime import datetime
from typing import List, TypeVar, Optional, ContextManager, Iterable, Union

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, desc
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import Selectable
from sqlalchemy.util.compat import contextmanager

from quaentropy.api.data_reader import (
    DataReader,
    ExperimentRecord,
    ResultRecord,
    MetadataRecord,
    DebugRecord,
    PlotRecord,
)
from quaentropy.api.data_writer import (
    DataWriter,
    ExperimentInitialData,
    ExperimentEndData,
    RawResultData,
    Metadata,
    Debug,
    Plot,
)
from quaentropy.results_backend.sqlalchemy.model import (
    Base,
    ExperimentTable,
    PlotTable,
    ResultTable,
    DebugTable,
    MetadataTable,
)

_SQL_ALCHEMY_MEMORY = ":memory:"
T = TypeVar(
    "T",
)


class SqlalchemySqlitePandasConnector(DataWriter, DataReader):
    def __init__(self, backend=None, echo=False):
        super(SqlalchemySqlitePandasConnector, self).__init__()
        if backend is None:
            backend = _SQL_ALCHEMY_MEMORY
        else:
            if backend != _SQL_ALCHEMY_MEMORY:
                dirname = os.path.dirname(backend)
                if dirname and dirname != "":
                    os.makedirs(dirname, exist_ok=True)
        backend = "sqlite:///" + backend
        self._engine = create_engine(backend, echo=echo)
        Base.metadata.create_all(self._engine)
        self._Session = sessionmaker(bind=self._engine)

    def save_experiment_initial_data(self, initial_data: ExperimentInitialData) -> int:
        transaction = ExperimentTable.from_initial_data(initial_data)
        return self._execute_transaction(transaction)

    def save_experiment_end_data(self, experiment_id: int, end_data: ExperimentEndData):
        with self._session_maker() as sess:
            query = (
                sess.query(ExperimentTable)
                .filter(ExperimentTable.id == int(experiment_id))
                .one_or_none()
            )
            if query:
                query.end_time = end_data.end_time
                query.success = end_data.success
                sess.flush()

    def save_result(self, experiment_id: int, result: RawResultData):
        transaction = ResultTable.from_model(experiment_id, result)
        return self._execute_transaction(transaction)

    def save_metadata(self, experiment_id: int, metadata: Metadata):
        transaction = MetadataTable.from_model(experiment_id, metadata)
        return self._execute_transaction(transaction)

    def save_debug(self, experiment_id: int, debug: Debug):
        transaction = DebugTable.from_model(experiment_id, debug)
        return self._execute_transaction(transaction)

    def save_plot(self, experiment_id: int, plot: Plot):
        transaction = PlotTable.from_model(experiment_id, plot)
        return self._execute_transaction(transaction)

    def get_experiments_range(self, starting_from_index: int, count: int) -> DataFrame:
        with self._session_maker() as sess:
            query = sess.query(ExperimentTable).slice(
                starting_from_index, starting_from_index + count
            )
            return self._query_pandas(query)

    def get_experiment_record(self, experiment_id: int) -> Optional[ExperimentRecord]:
        with self._session_maker() as sess:
            query = (
                sess.query(ExperimentTable)
                .filter(ExperimentTable.id == int(experiment_id))
                .one_or_none()
            )
            if query:
                return query.to_record()

    def get_experiments(
        self,
        label: Optional[str] = None,
        start_after: Optional[datetime] = None,
        end_after: Optional[datetime] = None,
        success: Optional[bool] = None,
    ) -> Iterable[ExperimentRecord]:
        with self._session_maker() as sess:
            query = sess.query(ExperimentTable)
            if label:
                query = query.filter(ExperimentTable.label == label)
            if success:
                query = query.filter(ExperimentTable.success == success)
            if start_after:
                query = query.filter(ExperimentTable.start_time > start_after)
            if end_after:
                query = query.filter(ExperimentTable.end_time > end_after)
            return [item.to_record() for item in query.all()]

    def get_results(
        self,
        experiment_id: Optional[int] = None,
        label: Optional[str] = None,
        stage: Optional[int] = None,
    ) -> Iterable[ResultRecord]:
        with self._session_maker() as sess:
            query = sess.query(ResultTable)
            if experiment_id:
                query = query.filter(ResultTable.experiment_id == int(experiment_id))
            if label:
                query = query.filter(ResultTable.label == label)
            if stage:
                query = query.filter(ResultTable.stage == stage)
            return [item.to_record() for item in query.all()]

    def get_metadata_records(
        self,
        experiment_id: Optional[int] = None,
        label: Optional[str] = None,
        stage: Optional[int] = None,
    ) -> Iterable[MetadataRecord]:
        with self._session_maker() as sess:
            query = sess.query(MetadataTable)
            if experiment_id:
                query = query.filter(MetadataTable.experiment_id == int(experiment_id))
            if label:
                query = query.filter(MetadataTable.label == label)
            if stage:
                query = query.filter(MetadataTable.stage == stage)
            return [item.to_record() for item in query.all()]

    def get_debug_record(self, experiment_id: int) -> Optional[DebugRecord]:
        with self._session_maker() as sess:
            query = (
                sess.query(MetadataTable)
                .filter(DebugTable.experiment_id == int(experiment_id))
                .one_or_none()
            )
            if query:
                return query.to_record()

    def get_all_results_with_label(self, exp_id, name) -> DataFrame:
        with self._session_maker() as sess:
            query = (
                sess.query(ResultTable)
                .filter(ResultTable.experiment_id == int(exp_id))
                .filter(ResultTable.label == name)
            )
            return self._query_pandas(query)

    def get_plots(self, experiment_id: int) -> List[PlotRecord]:
        with self._session_maker() as sess:
            query = (
                sess.query(PlotTable)
                .filter(PlotTable.experiment_id == int(experiment_id))
                .all()
            )
            if query:
                return [plot.to_record() for plot in query]
        return []

    def get_last_result_of_experiment(
        self, experiment_id: int
    ) -> Optional[ResultRecord]:
        with self._session_maker() as sess:
            query = (
                sess.query(ResultTable)
                .filter(ResultTable.experiment_id == int(experiment_id))
                .order_by(desc(ResultTable.time))
                .first()
            )
            if query:
                return query.to_record()

    def custom_query(self, query: Union[str, Selectable]) -> DataFrame:
        with self._session_maker() as sess:
            if isinstance(query, str):
                selectable = query
            else:
                selectable = query.statement

            return pd.read_sql(selectable, sess.bind)

    def _execute_transaction(self, transaction):
        with self._session_maker() as sess:
            sess.add(transaction)
            sess.flush()
            return transaction.id

    @staticmethod
    def _query_pandas(query):
        return pd.read_sql(query.statement, query.session.bind)

    @contextmanager
    def _session_maker(self) -> ContextManager[Session]:
        """Provide a transactional scope around a series of operations."""
        session = self._Session()
        try:
            yield session
            session.commit()
        except DBAPIError:
            session.rollback()
            raise
        finally:
            session.close()

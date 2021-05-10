import os
from datetime import datetime
from typing import List, TypeVar, Optional, ContextManager, Iterable, Union, Any
from typing import Set

import jsonpickle
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, desc
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import Selectable
from sqlalchemy.util.compat import contextmanager

from entropylab.api.data_reader import (
    DataReader,
    ExperimentRecord,
    ResultRecord,
    MetadataRecord,
    DebugRecord,
    PlotRecord,
)
from entropylab.api.data_writer import (
    DataWriter,
    ExperimentInitialData,
    ExperimentEndData,
    RawResultData,
    Metadata,
    Debug,
    PlotSpec,
    NodeData,
)
from entropylab.instruments.instrument_driver import Function, Parameter
from entropylab.instruments.lab_topology import (
    PersistentLabDB,
    DriverType,
    ResourceRecord,
)
from entropylab.results_backend.sqlalchemy.lab_model import (
    Resources,
    ResourcesSnapshots,
)
from entropylab.results_backend.sqlalchemy.model import (
    Base,
    ExperimentTable,
    PlotTable,
    ResultTable,
    DebugTable,
    MetadataTable,
    NodeTable,
)

_SQL_ALCHEMY_MEMORY = ":memory:"
T = TypeVar(
    "T",
)


class SqlAlchemyDB(DataWriter, DataReader, PersistentLabDB):
    """
    Database implementation using SqlAlchemy package for results (DataWriter
     and DataReader) and lab resources (PersistentLabDB)
    """

    def __init__(self, path=None, echo=False):
        """
            Database implementation using SqlAlchemy package for results (DataWriter
            and DataReader) and lab resources (PersistentLabDB)
        :param path: database file path (absolute or relative)
        :param echo: if True, the database engine will log all statements
        """
        super(SqlAlchemyDB, self).__init__()
        if path is None:
            path = _SQL_ALCHEMY_MEMORY
        else:
            if path != _SQL_ALCHEMY_MEMORY:
                dirname = os.path.dirname(path)
                if dirname and dirname != "":
                    os.makedirs(dirname, exist_ok=True)
        path = "sqlite:///" + path
        self._engine = create_engine(path, echo=echo)
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

    def save_plot(self, experiment_id: int, plot: PlotSpec, data: Any):
        transaction = PlotTable.from_model(experiment_id, plot, data)
        return self._execute_transaction(transaction)

    def save_node(self, experiment_id: int, node_data: NodeData):
        transaction = NodeTable.from_model(experiment_id, node_data)
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
            if label is not None:
                query = query.filter(ExperimentTable.label == label)
            if success is not None:
                query = query.filter(ExperimentTable.success == success)
            if start_after is not None:
                query = query.filter(ExperimentTable.start_time > start_after)
            if end_after is not None:
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
            if experiment_id is not None:
                query = query.filter(ResultTable.experiment_id == int(experiment_id))
            if label is not None:
                query = query.filter(ResultTable.label == str(label))
            if stage is not None:
                query = query.filter(ResultTable.stage == int(stage))
            return [item.to_record() for item in query.all()]

    def get_metadata_records(
        self,
        experiment_id: Optional[int] = None,
        label: Optional[str] = None,
        stage: Optional[int] = None,
    ) -> Iterable[MetadataRecord]:
        with self._session_maker() as sess:
            query = sess.query(MetadataTable)
            if experiment_id is not None:
                query = query.filter(MetadataTable.experiment_id == int(experiment_id))
            if label is not None:
                query = query.filter(MetadataTable.label == label)
            if stage is not None:
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

    def get_nodes_id_by_label(
        self, label: str, experiment_id: Optional[int] = None
    ) -> List[int]:
        with self._session_maker() as sess:
            query = sess.query(NodeTable).filter(NodeTable.label == label)
            if experiment_id is not None:
                query = query.filter(NodeTable.experiment_id == int(experiment_id))

            result = query.all()
            if result:
                return [node.id for node in result]
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

    def save_new_resource_driver(
        self,
        name: str,
        driver_source_code: str,
        module: str,
        class_name: str,
        serialized_args: str,
        serialized_kwargs: str,
        number_of_experiment_args: int,
        keys_of_experiment_kwargs: List[str],
        functions: List[Function],
        parameters: List[Parameter],
        undeclared_functions: List[Function],
    ):
        transaction = Resources(
            update_time=datetime.now(),
            name=name,
            driver=driver_source_code,
            module=module,
            class_name=class_name,
            version=1,
            driver_type=DriverType.Packaged,
            args=serialized_args,
            kwargs=serialized_kwargs,
            number_of_experiment_args=number_of_experiment_args,
            keys_of_experiment_kwargs=", ".join(keys_of_experiment_kwargs),
            cached_metadata=jsonpickle.dumps(
                {
                    "functions": functions,
                    "undeclared_functions": undeclared_functions,
                    "parameters": parameters,
                }
            ),
        )
        return self._execute_transaction(transaction)

    def remove_resource(self, name: str):
        with self._session_maker() as sess:
            query = (
                sess.query(Resources)
                .filter(Resources.name == name)
                .order_by(desc(Resources.update_time))
                .first()
            )
            if query:
                query.deleted = True
                sess.flush()

    def save_state(self, name: str, state: str, snapshot_name: str):
        driver_id = self._get_driver_id(name)
        transaction = ResourcesSnapshots(
            update_time=datetime.now(),
            name=snapshot_name,
            driver_id=driver_id,
            state=state,
        )
        return self._execute_transaction(transaction)

    def get_state(self, resource_name: str, snapshot_name: str) -> str:
        driver_id = self._get_driver_id(resource_name)
        with self._session_maker() as sess:
            query = (
                sess.query(ResourcesSnapshots)
                .filter(ResourcesSnapshots.driver_id == driver_id)
                .filter(ResourcesSnapshots.name == snapshot_name)
                .order_by(desc(ResourcesSnapshots.update_time))
                .first()
            )
            if query:
                return query.state
            else:
                return ""

    def get_all_states(self, name) -> Iterable[str]:
        driver_id = self._get_driver_id(name)
        with self._session_maker() as sess:
            query = (
                sess.query(ResourcesSnapshots)
                .filter(ResourcesSnapshots.driver_id == driver_id)
                .order_by(desc(ResourcesSnapshots.update_time))
            )
            return query.all()

    def get_resource(self, name) -> Optional[ResourceRecord]:
        with self._session_maker() as sess:
            query = (
                sess.query(Resources)
                .filter(Resources.name == name)
                .order_by(desc(Resources.update_time))
                .first()
            )
            if query and not query.deleted:
                cached_metadata = jsonpickle.loads(query.cached_metadata)
                return ResourceRecord(
                    query.name,
                    query.module,
                    query.class_name,
                    query.driver,
                    query.version,
                    query.driver_type,
                    query.args,
                    query.kwargs,
                    cached_metadata["functions"],
                    cached_metadata["parameters"],
                    cached_metadata["undeclared_functions"],
                )
            else:
                return None

    def _get_driver_id(self, name) -> int:
        with self._session_maker() as sess:
            query = (
                sess.query(Resources)
                .filter(Resources.name == name)
                .order_by(desc(Resources.update_time))
                .first()
            )
            if query and not query.deleted:
                return int(query.id)
            else:
                return -1

    def set_locked(self, resource_name):
        pass

    def set_released(self, resource_name):
        pass

    def get_all_resources(self) -> Set[str]:
        with self._session_maker() as sess:
            query = (
                sess.query(Resources)
                .filter(Resources.deleted == False)  # noqa: E712
                .group_by(Resources.name)
                .all()
            )
            if query is not None:
                return {item.name for item in query}
            else:
                return set()

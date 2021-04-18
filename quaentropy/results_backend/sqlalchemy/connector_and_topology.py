from datetime import datetime
from typing import Iterable, Optional

from sqlalchemy import (
    Column,
    Integer,
    String,
    DATETIME,
    ForeignKey,
    desc,
    Enum,
    Boolean,
)

from quaentropy.instruments.lab_topology import (
    PersistentLabDB,
    DriverType,
    ResourceRecord,
)
from quaentropy.results_backend.sqlalchemy.connector import (
    SqlalchemySqlitePandasConnector,
)
from quaentropy.results_backend.sqlalchemy.model import Base


class Topology(Base):
    __tablename__ = "Topology"

    id = Column(Integer, primary_key=True)
    update_time = Column(DATETIME, nullable=False)
    name = Column(String)
    driver = Column(String)
    module = Column(String)
    class_name = Column(String)
    version = Column(String)
    driver_type = Column(Enum(DriverType))
    args = Column(String)
    kwargs = Column(String)
    deleted = Column(Boolean, default=False)


class TopologySnapshots(Base):
    __tablename__ = "TopologySnapshots"

    id = Column(Integer, primary_key=True)
    update_time = Column(DATETIME, nullable=False)
    name = Column(String)
    driver_id = Column(Integer, ForeignKey("Topology.id", ondelete="CASCADE"))
    state = Column(String)


class SqlalchemySqlitePandasAndTopologyConnector(
    SqlalchemySqlitePandasConnector, PersistentLabDB
):
    def __init__(self, backend=None, echo=False):
        super().__init__(backend, echo)

    def save_new_resource_driver(
        self,
        name: str,
        driver_source_code: str,
        module: str,
        class_name: str,
        serialized_args: str,
        serialized_kwargs: str,
    ):
        transaction = Topology(
            update_time=datetime.now(),
            name=name,
            driver=driver_source_code,
            module=module,
            class_name=class_name,
            version=1,  # TODO
            driver_type=DriverType.Packaged,
            args=serialized_args,
            kwargs=serialized_kwargs,
        )
        return self._execute_transaction(transaction)

    def remove_resource(self, name: str):
        with self._session_maker() as sess:
            query = (
                sess.query(Topology)
                .filter(Topology.name == name)
                .order_by(desc(Topology.update_time))
                .first()
            )
            if query:
                query.deleted = True
                sess.flush()

    def save_state(self, name: str, state: str, snapshot_name: str):
        driver_id = self._get_driver_id(name)
        transaction = TopologySnapshots(
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
                sess.query(TopologySnapshots)
                .filter(TopologySnapshots.driver_id == driver_id)
                .filter(TopologySnapshots.name == snapshot_name)
                .order_by(desc(TopologySnapshots.update_time))
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
                sess.query(TopologySnapshots)
                .filter(TopologySnapshots.driver_id == driver_id)
                .order_by(desc(TopologySnapshots.update_time))
            )
            return query.all()

    def get_resource(self, name) -> Optional[ResourceRecord]:
        with self._session_maker() as sess:
            query = (
                sess.query(Topology)
                .filter(Topology.name == name)
                .order_by(desc(Topology.update_time))
                .first()
            )
            if query and not query.deleted:
                return ResourceRecord(
                    query.name,
                    query.module,
                    query.class_name,
                    query.driver,
                    query.version,
                    query.driver_type,
                    query.args,
                    query.kwargs,
                )
            else:
                return None

    def _get_driver_id(self, name) -> int:
        with self._session_maker() as sess:
            query = (
                sess.query(Topology)
                .filter(Topology.name == name)
                .order_by(desc(Topology.update_time))
                .first()
            )
            if query and not query.deleted:
                return int(query.id)
            else:
                return -1

    def set_locked(self, resource_name):
        pass  # todo

    def set_released(self, resource_name):
        pass  # todo

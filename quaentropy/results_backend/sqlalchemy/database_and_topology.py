from datetime import datetime

from sqlalchemy import Column, Integer, String, DATETIME, ForeignKey, desc

from quaentropy.instruments.lab_topology import LabTopologyBackend
from quaentropy.results_backend.sqlalchemy.database import (
    SqlalchemySqlitePandasConnector,
)
from quaentropy.results_backend.sqlalchemy.model import Base


class Topology(Base):
    __tablename__ = "Topology"

    id = Column(Integer, primary_key=True)
    update_time = Column(DATETIME, nullable=False)
    name = Column(String)
    driver = Column(String)


class TopologyStates(Base):
    __tablename__ = "TopologyStates"

    id = Column(Integer, primary_key=True)
    update_time = Column(DATETIME, nullable=False)
    driver_id = Column(Integer, ForeignKey("Topology.id", ondelete="CASCADE"))
    state = Column(String)


class SqlalchemySqlitePandasAndTopologyConnector(
    SqlalchemySqlitePandasConnector, LabTopologyBackend
):
    def __init__(self, backend=None, echo=False):
        super().__init__(backend, echo)

    def save_driver(self, name: str, driver: str):
        transaction = Topology(
            update_time=datetime.now(),
            name=name,
            driver=driver,
        )
        return self._execute_transaction(transaction)

    def save_state(self, name: str, state: str):
        driver_id = self._get_driver_id(name)
        transaction = TopologyStates(
            update_time=datetime.now(),
            driver_id=driver_id,
            state=state,
        )
        return self._execute_transaction(transaction)

    def get_latest_state(self, name) -> str:
        driver_id = self._get_driver_id(name)
        with self._session_maker() as sess:
            query = (
                sess.query(TopologyStates)
                .filter(TopologyStates.driver_id == driver_id)
                .order_by(desc(TopologyStates.update_time))
                .first()
            )
            if query:
                return query.state
            else:
                return ""

    def _get_driver_id(self, name) -> int:
        with self._session_maker() as sess:
            query = sess.query(Topology).filter(Topology.name == name).one_or_none()
            if query:
                return int(query.id)
            else:
                return -1

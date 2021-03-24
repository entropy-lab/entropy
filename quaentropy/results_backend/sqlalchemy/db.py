from quaentropy.results_backend.sqlalchemy.connector_and_topology import (
    SqlalchemySqlitePandasAndTopologyConnector,
)


class SqlAlchemyDB(SqlalchemySqlitePandasAndTopologyConnector):
    def __init__(self, backend=None, echo=False):
        super().__init__(backend, echo)

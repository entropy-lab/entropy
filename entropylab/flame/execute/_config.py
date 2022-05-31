import logging


class _Config:
    port_number = 9000
    node_status_dict = {}
    execution_active = True
    job_id = None
    DATABASE_NAME = None
    SQLALCHEMY_DATABASE_URL = None


logger = logging.getLogger("ExecuteLogger")

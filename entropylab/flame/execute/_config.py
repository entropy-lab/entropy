import os
import logging


class _Config:
    port_number = 9000
    node_status_dict = {}
    execution_active = True
    job_eui = None
    runtime_id = None
    DATABASE_NAME = None
    SQLALCHEMY_DATABASE_URL = None


flame_user = os.environ.get("FLAME_MESSAGING_USER_NAME", "flame_user")
flame_password = os.environ.get("FLAME_MESSAGING_USER_PASS", "flame_password")
port = os.environ.get("FLAME_MESSAGING_PORT", "5672")
node_debugging_user = os.environ.get(
    "NODE_DEBUG_MESSAGING_USER_NAME", "nodes_debug_user"
)
node_debugging_password = os.environ.get(
    "NODE_DEBUG_MESSAGING_USER_PASS", "nodes_debug_password"
)

logger = logging.getLogger("ExecuteLogger")

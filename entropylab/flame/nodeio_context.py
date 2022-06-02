import zmq
from sqlalchemy import create_engine

__all__ = [
    "zmq_context",
    "status",
    "node_name",
    "node_description",
    "node_dependancies",
    "entropy_identity",
    "playbook",
    "databucket",
    "data_server",
]

status = None
node_name = ""
node_description = ""
node_dependancies = []
node_icon = ""
entropy_identity = None
playbook = None
databucket = None
data_server = None

zmq_context_variable = None
runtime_data = None
executor_input = None  #: address of executor zmq communication channel
executor_output = None


def zmq_context():
    global zmq_context_variable
    if zmq_context_variable is None:
        zmq_context_variable = zmq.Context()
    return zmq_context_variable


def runtime_db_connect():
    global runtime_data
    SQLALCHEMY_DATABASE_URL = (
        f"postgresql://{data_server[0]}:{data_server[1]}"
        + f'@{data_server[2]}/{data_server[3]}?client_encoding="utf8"'
    )
    runtime_data = create_engine(SQLALCHEMY_DATABASE_URL).connect()


def runtime_db_close():
    runtime_data.close()

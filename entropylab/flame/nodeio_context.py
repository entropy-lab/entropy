import zmq
import json
import os
import subprocess
import platform
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
    "save_dry_run",
    "dry_run_data",
    "dry_run_log",
    "terminate_node",
    "is_IPython",
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

save_dry_run = False
dry_run_data = None
dry_run_log = None


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


def is_IPython():
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter


def terminate_node():
    if save_dry_run and runtime_data is None:
        with open("dry_run_data.json", "w") as outfile:
            json.dump(dry_run_data, outfile)
        dry_run_log.close()
        # save to entropyhub
        env = os.environ
        env["PYTHONPATH"] = os.getcwd()
        if platform.system() == "Windows":
            python_cmd = "python"
        else:
            python_cmd = "python3"
        subprocess.run(
            f"{python_cmd} -m entropyhub.submit_dry_run",
            env=env,
            shell=True,
            universal_newlines=True,
            start_new_session=True,
        )

    if is_IPython():

        class StopExecution(Exception):
            def _render_traceback_(self):
                pass

        raise StopExecution
    else:
        exit()

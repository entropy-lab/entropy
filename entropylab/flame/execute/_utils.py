import json
import socket
import argparse

import zmq
import msgpack
import contextlib

from tzlocal import get_localzone
from datetime import datetime, timezone

from sqlalchemy import create_engine

from entropylab.flame.execute._config import _Config, logger


def is_port_in_use(port, host="localhost"):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        in_use = s.connect_ex((host, port)) == 0
        logger.debug(f"Utils. Check port {port} in use. Result: {in_use}. Host: {host}")
        return in_use


def status_update(node_name, routing_key, message, style, updates_channel):
    logger.debug(
        f"Utils. Status update. Node name: {node_name}; "
        f"routing key: {routing_key}; message: {message}; "
        f"style: {style}"
    )
    if updates_channel is not None:
        update = {"node": node_name, "msg": message, "style": style}
        update = json.dumps(update)
        updates_channel.basic_publish(
            exchange="amq.topic", routing_key=routing_key, body=update.encode()
        )
    _Config.node_status_dict[node_name] = message


def get_free_port(start_port_number, runtime_state, eui, end_port_number=64000):
    logger.debug(f"Utils. Get free port. Start port: {start_port_number}. EUI: {eui}")
    port_in_use = True
    port_number = start_port_number
    while port_in_use and port_number < end_port_number:
        port_number += 1
        # set lock by entropy if possible on this port
        if runtime_state.setnx(f"system/port{port_number}", eui):
            # lock set, is the port in use?
            port_in_use = is_port_in_use(port_number)
            if port_in_use:
                # port is in use
                # clear lock by entropy
                runtime_state.delete(f"system/port{port_number}")
    if port_number == end_port_number:
        raise ValueError("No free ports for connecting nodes found")
    logger.debug(f"Utils. Get free port. Founded port: {port_number}")
    return port_number


def remove_port_lock(port_number, runtime_state):
    logger.debug(f"Utils. Remove port lock. Port: {port_number}")
    runtime_state.delete(f"system/port{port_number}")


def exit_gracefully(self, *args):
    logger.debug("Utils. Exit gracefully")
    _Config.execution_active = False


def check_node_messages(executor_input):
    logger.debug("Utils. Check node messages.")
    try:
        # check for updates
        msg = msgpack.unpackb(executor_input.recv(flags=zmq.NOBLOCK))
        logger.debug(f"Utils. Check node messages. Message: {msg}")
        if msg["status"] == "requests workflow termination":
            _Config.execution_active = False
        return
    except zmq.error.ZMQError as e:
        if e.errno == zmq.EAGAIN:
            # the state value has not been updated since last
            # get call
            return
        else:
            logger.error(f"Utils. Check node messages. Exception: {str(e)}")
            return


@contextlib.contextmanager
def get_db_manager(db_url):
    engine = create_engine(db_url)  # SQLALCHEMY_DATABASE_URL
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


@contextlib.contextmanager
def get_runtimedata(db_name):
    engine = create_engine(db_name)  # DATABASE_NAME
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


def write_metadata_to_h5(h5file, metadata):
    logger.debug(f"Utils. Write metadata to h5 file. Metadata: {metadata}")
    h5file.attrs["project"] = metadata.get("project", "")
    h5file.attrs["job_eui"] = metadata.get("job_eui", "")
    h5file.attrs["job_description"] = metadata.get("job_description", "")
    h5file.attrs["workflow_eui"] = metadata.get("workflow_eui", "")
    h5file.attrs["parameters_eui"] = metadata.get("parameters_eui", "")
    if "workflow_commit_id" in metadata:
        h5file.attrs["workflow_commit"] = metadata.get("workflow_commit_id")
    if "parameters_commit_id" in metadata:
        h5file.attrs["parameters_commit"] = metadata.get("parameters_commit_id")

    utc_dt = datetime.now(timezone.utc)
    h5file.attrs["creation_timestamp"] = "{}".format(
        utc_dt.astimezone(get_localzone()).isoformat()
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Flame executor of parametrized workflows"
    )
    parser.add_argument(
        "-w",
        "--workflow",
        type=str,
        default="workflow.py",
        help="Python file that defines main entropylab.Workflow (default workflow.py)",
    )
    parser.add_argument(
        "-p",
        "--parameters",
        type=str,
        default="parameters.json",
        help="JSON file that resolves workflow parameters",
    )
    parser.add_argument(
        "-t",
        "--max-execution-time",
        type=int,
        default=0,
        help="Maximal execution time in s. Default 0 (no-limit).",
    )
    parser.add_argument(
        "-d",
        "--status-check-interval",
        type=int,
        default=1,
        help="Node status check interval in s (default 1 s)",
    )
    parser.add_argument(
        "-m",
        "--metadata",
        type=str,
        default='{"project": "test_from_command_line", '
        '"job_description": "Flame execution from command line"}',
        help="Metadata about the particular executed job in json string format consisting of "
        "project prefix, runtime id, job eui, workflow eui, parameters eui, "
        "workflow commit hash, parameters commit hash, job description. For example: "
        '\'{"project": "default_project", "runtime_id": 1, "job_eui": "#/j1", '
        '"workflow_eui": "#/w1", "parameters_eui": "#/p1", '
        '"workflow_commit_id": "b3a8f6973ea45a1fc34bfd83e1e67b90066253eb", '
        '"parameters_commit_id": "9c41a22a6502ad4b26f63011f5701ac4c5ef1861", '
        '"job_description": "some description"}\'',
    )
    parser.add_argument(
        "-c",
        "--connection-wait",
        type=int,
        default=60,
        help="How long in seconds to wait for succesful establishment of "
        "communication between nodes before timeout. Default 60.",
    )
    return parser.parse_args()

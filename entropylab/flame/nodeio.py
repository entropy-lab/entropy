from .inputs import Inputs
from .outputs import Outputs
import os
import sys
import signal
import json
import __main__
import subprocess
import argparse
import redis
import time
import zmq
import msgpack
import platform
from . import nodeio_context
from .nodeio_context import terminate_node

__all__ = ["Inputs", "Outputs", "register", "terminate_workflow", "terminate_node"]


def context(name="", description="", icon=""):
    """Description of the current node in the wider context.

    :param name: Name that will be used for providing instances of this node
      in the workflow. It should start with upper case and not contain
      spaces or special characters. E.g. valid name is ExampleName
    :param description: Description of functionality of the node. It will be
      used as documentation of the node.
    :param icon: Optional, icon to use. Icons have to be available in
      entropy_web_ui/nodeicons. Use e.g. 'bootstrap/alarm.svg'
    """

    global status
    global node_name
    global node_description
    global node_dependancies
    global entropy_identity

    parser = argparse.ArgumentParser(prog="Entropy node")
    parser.add_argument("--entropy-identity", type=str, default=None)
    parser.add_argument("--entropy-playbook", type=str, default=None)
    parser.add_argument("args", nargs=argparse.REMAINDER)
    args, _unknown = parser.parse_known_args()

    if args.entropy_identity is not None:
        print("=" * 40)
        print("|- Entropy lab context")
        print(f"|   Node identity:   #{args.entropy_identity}")
        nodeio_context.entropy_identity = args.entropy_identity
    if args.entropy_playbook is not None:
        print(f"|   Playbook:        {args.entropy_playbook}")
        playbook_location = args.entropy_playbook.split(",")
        nodeio_context.playbook = redis.Redis(
            host=playbook_location[0],
            port=playbook_location[1],
            db=playbook_location[2],
        )
        input_values = json.loads(
            nodeio_context.playbook.get(f"#{args.entropy_identity}").decode()
        )
        print("=" * 40)

        external_inputs = Inputs()
        for key, value in input_values.items():
            # Note: type of the variable is not important now, it will be overwritten
            # by the node. We will set initiall all inputs to be states.
            external_inputs.state(key)
            # resolve relative refences for communication if needed
            if type(value) == str and len(value) > 0 and value[0] == "#":
                input_values[key] = "#" + nodeio_context.playbook.get(value).decode()
        external_inputs.set(**input_values)
        Inputs._set_as_external_input(external_inputs)

        nodeio_context.data_server = (
            nodeio_context.playbook.get("dataserver").decode().split(",")
        )
        nodeio_context.runtime_db_connect()
        nodeio_context.node_icon = icon

        zmq_context = nodeio_context.zmq_context()

        executor_input_address = nodeio_context.playbook.get("executor_input").decode()
        socket = zmq_context.socket(zmq.PUB)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.IMMEDIATE, 1)
        socket.connect(executor_input_address)
        nodeio_context.executor_input = socket

        executor_output_address = nodeio_context.playbook.get(
            "executor_output"
        ).decode()
        socket = zmq_context.socket(zmq.SUB)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.IMMEDIATE, 1)
        socket.connect(executor_output_address)
        socket.subscribe("")
        nodeio_context.executor_output = socket

    nodeio_context.node_name = name
    nodeio_context.node_description = description
    nodeio_context.node_icon = icon
    status = StateMachine()


class StateMachine:
    active = True

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.active = False
        terminate_node()

    # add status to be executable property; and allow blocking on
    # status check in case we are resolving calibration of the system


def register(save_dry_run=False):
    """Saves this into node library, allowing it to be used in workflows.

    Args:
        save_dry_run (bool): Optional, should dry-run of the node be saved into
        EntropyHub as a single-node workflow, with corresponding jobId and HDF5.
    """

    if nodeio_context.playbook is not None:
        # wait to establish connections
        upstream_connections = {}
        upstream_data = {}
        outstanding_connections = 0

        for key, value in Inputs._connections().items():
            outstanding_connections += 1
            upstream_connections[key] = value
            upstream_data[key] = None

        outstanding_connections += 1
        upstream_connections["executor_output#"] = nodeio_context.executor_output
        upstream_data["executor_output#"] = None

        while (
            upstream_data["executor_output#"] is None
            or upstream_data["executor_output#"] == ""
        ):
            # send connection ping on all outputs
            for _, out in Outputs._connections().items():
                out.send(msgpack.packb(""))
            # wait
            time.sleep(0.1)
            # non blocking connection ping receive on all inputs
            for key, conn in upstream_connections.items():
                try:
                    a = msgpack.unpackb(conn.recv(flags=zmq.NOBLOCK))
                    if upstream_data[key] is None:
                        outstanding_connections -= 1
                    upstream_data[key] = a
                except zmq.error.ZMQError as e:
                    if e.errno == zmq.EAGAIN:
                        pass
                    else:
                        raise Exception(repr(e))

            # if all incoming connections are established ping ready to executor
            if outstanding_connections == 0:
                registration = {
                    "eui": nodeio_context.entropy_identity,
                    "status": "connected",
                }
                nodeio_context.executor_input.send(msgpack.packb(registration))

            # if executor have not send stop repeat from ping on outputs

        # if executor send stop, stop and ping back you stopped
        registration = {
            "eui": nodeio_context.entropy_identity,
            "status": "waiting_flush",
        }
        nodeio_context.executor_input.send(msgpack.packb(registration))

        # wait for executor to send flush buffer command
        executor = msgpack.unpackb(nodeio_context.executor_output.recv())
        if executor["cmd"] != "flush":
            raise ValueError(
                "Terminating... received cmd from executor ", executor["cmd"]
            )
        # flush and ping back to executor you are ready
        for _key, conn in upstream_connections.items():
            buffer_full = True
            while buffer_full:
                try:
                    a = conn.recv(flags=zmq.NOBLOCK)
                except zmq.error.ZMQError as e:
                    if e.errno == zmq.EAGAIN:
                        buffer_full = False
                    else:
                        raise Exception(repr(e))

        # wait for command to start execution now
        registration = {"eui": nodeio_context.entropy_identity, "status": "ready"}
        nodeio_context.executor_input.send(msgpack.packb(registration))

        # block untill all nodes are up
        executor = msgpack.unpackb(nodeio_context.executor_output.recv())
        if executor["cmd"] != "start":
            raise ValueError(
                "Terminating... received cmd from executor ", executor["cmd"]
            )
        # TO-DO - check that saved node schema is the same as the current node
        # schema (i.e. node code has not been changed since the last time we
        # indenpendantly run this node)

        return  # We are running as a part of the workflow. Skip node creation.

    if nodeio_context.is_IPython():
        return  # this node is still being prepared for runtime

    bin_path = str(os.path.basename(__main__.__file__))

    schema = {
        "name": nodeio_context.node_name,
        "description": nodeio_context.node_description,
        "command": "python3",
        "bin": bin_path,
        "icon": nodeio_context.node_icon,
        "inputs": Inputs._schema(),
        "outputs": Outputs._schema(),
    }

    nodeio_context.save_dry_run = save_dry_run
    if nodeio_context.save_dry_run:
        nodeio_context.dry_run_data = {}
        nodeio_context.dry_run_data[
            "job_description"
        ] = f"Dry run of {nodeio_context.node_name}"
        node = {}
        node["name"] = nodeio_context.node_name
        node["description"] = nodeio_context.node_description
        node["bin"] = bin_path
        node["outputs"] = {}
        nodeio_context.dry_run_data["node"] = node
        nodeio_context.dry_run_log = open("dry_run.log", "w")
        sys.stdout = nodeio_context.dry_run_log

    destination = os.path.join(os.path.join(os.getcwd(), "entropynodes"), "schema")
    if not os.path.exists(destination):
        os.makedirs(destination)
    with open(
        os.path.join(destination, f"{nodeio_context.node_name}.json"),
        encoding="utf-8",
        mode="w",
    ) as f:
        f.write(json.dumps(schema))

    env = os.environ
    env["PYTHONPATH"] = os.getcwd()

    if platform.system() == "Windows":
        python_cmd = "python"
    else:
        python_cmd = "python3"

    subprocess.run(
        f"{python_cmd} -m entropylab.flame.generate_node {nodeio_context.node_name}",
        env=env,
        shell=True,
        universal_newlines=True,
        start_new_session=True,
    )


def terminate_workflow():
    if nodeio_context.playbook is not None:
        # request executor to terminate
        msg = {
            "eui": nodeio_context.entropy_identity,
            "status": "requests workflow termination",
        }
        nodeio_context.executor_input.send(msgpack.packb(msg))
        nodeio_context.runtime_db_close()
        nodeio_context.zmq_context().term()
        status.active = False
        terminate_node()
    else:
        # terminate just this node
        status.active = False
        terminate_node()

# Flame executor

import os
import sys
import time
import json
import psutil
import importlib
import platform

import zmq
import h5py
import msgpack

from sqlalchemy import text as sql_text

from entropylab.flame.workflow import Workflow
from entropylab.flame.execute import _utils as execute_utils
from entropylab.flame.execute._config import _Config, logger
from entropylab.flame.execute._message_queue_info import MessageQueueInfo
from entropylab.flame.execute._runtime_state_info import RuntimeStateInfo


class Execute:
    def __init__(self, args_):
        self.args = args_
        self.executor_input = None
        self.executor_output = None
        self.port_input = None
        self.port_output = None
        self.pid2node = {}
        self.node2process = {}
        self.processes_list = []
        self.cwd = os.getcwd()
        self.metadata = json.loads(self.args.metadata)
        self.__init_workflow()

        _Config.job_id = self.metadata.get("job_eui", "#/").replace("#/", "")
        if _Config.job_id == "":
            # for the Flame executed from command line without job EUI
            _Config.job_id = "output_data"
        runtime_id = self.metadata.get("runtime_id", -1)
        self.routing_key = f"status_updates.{runtime_id}.{_Config.job_id}"
        self.runtime_state_info = RuntimeStateInfo()
        # env variables FLAME_MESSAGING_USER_NAME and FLAME_MESSAGING_USER_PASS
        # describes connection credentials for message queue
        self.message_queue_info = MessageQueueInfo()

    def __init_workflow(self):
        logger.info("Execute. Init workflow")
        workflow_path = self.args.workflow.replace(".py", "")
        importlib.import_module(workflow_path)
        self.workflow = Workflow._main_workflow()
        # now prevent workflow to overwrite parameters on exit
        Workflow._prevent_parameter_file_overwrite()

        self.workflow._resolve_parameters(self.args.parameters)

    def __wait_for_processes(
        self,
        additional_process_list,
        message: str,
        style: str,
        wait_process_timeout: float = 3,
    ):
        logger.debug(
            f"Execute. Wait for processes. "
            f"Processes list: {self.processes_list}; "
            f"additional list: {additional_process_list}; "
            f"message: {message}; style: {style}; "
            f"wait timeout: {wait_process_timeout}."
        )
        gone, alive = psutil.wait_procs(
            self.processes_list + additional_process_list, timeout=wait_process_timeout
        )
        for g in gone:
            if g.pid in self.pid2node:
                node_name = self.pid2node[g.pid]
                execute_utils.status_update(
                    node_name,
                    self.routing_key,
                    message,
                    style,
                    self.message_queue_info.updates_channel,
                )
                code = g.returncode
                if code is not None and code != 0 and code != 15:
                    # code == 15 is for forceful termination since on Windows
                    # there is no graceful termination signal
                    # see https://bugs.python.org/issue26350
                    execute_utils.status_update(
                        node_name,
                        self.routing_key,
                        f"error, exit code {code}",
                        "error",
                        self.message_queue_info.updates_channel,
                    )
                self.processes_list.remove(g)
        return gone, alive

    def __setup_executor_input_output(self, port_number=9000):
        logger.debug(
            f"Execute. Setup executor input and output. Port number: {port_number}"
        )
        # open port for executor in and out channel
        zmq_context = zmq.Context()
        runtime_state = self.runtime_state_info.runtime_state
        port_number = execute_utils.get_free_port(
            port_number, runtime_state, "executor_input"
        )
        logger.debug(
            f"Execute. Setup executor input and output. "
            f"Input port number: {port_number}."
        )
        self.port_input = port_number
        port_address = f"tcp://127.0.0.1:{port_number}"
        runtime_state.set("executor_input", port_address)

        self.executor_input = zmq_context.socket(zmq.SUB)
        self.executor_input.setsockopt(zmq.LINGER, 0)
        self.executor_input.setsockopt(zmq.RCVTIMEO, 200)  # 0.2 s timeout
        self.executor_input.bind(port_address)  # receives from many publishers
        self.executor_input.subscribe("")

        port_number = execute_utils.get_free_port(
            port_number, runtime_state, "executor_output"
        )
        logger.debug(
            f"Execute. Setup executor input and output. "
            f"Output port number: {port_number}"
        )
        port_address = f"tcp://127.0.0.1:{port_number}"
        runtime_state.set("executor_output", port_address)
        self.port_output = port_number

        self.executor_output = zmq_context.socket(zmq.PUB)
        self.executor_output.setsockopt(zmq.LINGER, 0)
        self.executor_output.bind(port_address)  # sends to many publishers

    def __zmq_messages_status_check(
        self,
        total_node_count,
        connection_wait,
        status_check,
        need_send_output_msg=False,
    ):
        logger.debug(
            f"Execute. ZMQ messages status check. "
            f"Node count: {total_node_count}; "
            f"connection wait: {connection_wait}; "
            f"Status check: {status_check};"
            f"Need send output: {need_send_output_msg}."
        )
        ready_nodes_count = 0
        start_time_ = time.time()
        while (
            ready_nodes_count < total_node_count
            and _Config.execution_active
            and time.time() - start_time_ < connection_wait
        ):
            if need_send_output_msg:
                self.executor_output.send(msgpack.packb(""))

            try:
                update_ = msgpack.unpackb(self.executor_input.recv())
                if _Config.node_status_dict[update_["eui"]] != update_["status"]:
                    _Config.node_status_dict[update_["eui"]] = update_["status"]
                    if update_["status"] == status_check:
                        ready_nodes_count += 1
            except zmq.error.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    pass
        logger.debug(
            f"Execute. ZMQ messages status check. "
            f"Ready nodes count: {ready_nodes_count}"
        )
        return ready_nodes_count

    def __init_nodes(self, node_schemas_):
        # initialize all the nodes, saving process id
        logger.debug(f"Execute. Init nodes. Node schemas: {node_schemas_}.")
        log_directory = os.path.join(self.cwd, "entropylogs")
        playbook_server_arg = (
            f"{self.runtime_state_info.playbook_server[0]},"
            f"{self.runtime_state_info.playbook_server[1]},"
            f"{self.runtime_state_info.playbook_server[2]}"
        )
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        try:
            for node_name, node in self.workflow._nodes.items():
                node_class = type(node).__name__
                schema = node_schemas_[node_class]
                log_file = os.path.join(log_directory, f"{node_name}.txt")
                if platform.system() == "Windows" and (
                    schema["command"] == "python" or schema["command"] == "python3"
                ):
                    # -u is to unbuffer stoud and sterr and to write directly to log file
                    exec_command = "python -u"
                else:
                    exec_command = schema["command"]
                cmd = (
                    f'{exec_command} {os.path.join(self.cwd, schema["bin"])}'
                    + f' --entropy-identity "{node_name}"'
                    + f' --entropy-playbook "{playbook_server_arg}"'
                    + f' > "{log_file}" 2>&1'
                )
                p = psutil.Popen(
                    cmd, shell=True, universal_newlines=True, start_new_session=True
                )

                self.node2process[node_name] = p
                self.pid2node[p.pid] = node_name
                self.processes_list.append(p)
                self.runtime_state_info.runtime_state.set(f"pid#{node_name}", p.pid)

                execute_utils.status_update(
                    node_name,
                    self.routing_key,
                    "initialised",
                    "initialised",
                    self.message_queue_info.updates_channel,
                )

        except OSError as e:
            # the system failed to execute the shell
            # (out-of-memory, out-of-file-descriptors, and other extreme cases)
            logger.error(
                f"Execute. Init nodes. Fail to run shell.\n"
                f"etails: {str(e)}.\n"
                f"Traceback: {e.__traceback__}"
            )
            sys.exit("failed to run shell: '%s'" % (str(e)))

    def __assign_relative_outputs_to_free_ports(self):
        logger.debug("Execute. Assign relative outputs to free ports.")
        total_node_count = 0
        resolution = {}
        node_schemas = {}
        with execute_utils.get_runtimedata(_Config.DATABASE_NAME) as db:
            # check all relative outputs and assign them free ports
            # write resolution in playbook
            # TODO: if using UNIX compatible system,
            # and Windows 10 (https://github.com/zeromq/libzmq/issues/3691)
            # use ipc instead of tcp to reduce latency

            for node_name, node in self.workflow._nodes.items():
                total_node_count += 1
                node_info = self.workflow._node_details(node)
                for o in node._outputs._outputs:
                    eui = f"#{node_name}/{o}"

                    if node_info["outputs"][0]["retention"][o] > 0:
                        # store permanently this value
                        # create space in runtime database for storing this output
                        with db.begin():
                            db.execute(
                                sql_text(
                                    f"""
                                    CREATE TABLE "{eui}" (
                                    time TIMESTAMPTZ NOT NULL,
                                    value JSONB
                                    );
                                    """
                                )
                            )
                            db.execute(
                                sql_text(
                                    f"""SELECT create_hypertable('"{eui}"', 'time');"""
                                )
                            )

                    port_number = execute_utils.get_free_port(
                        _Config.port_number, self.runtime_state_info.runtime_state, eui
                    )

                    port_address = f"tcp://127.0.0.1:{port_number}"
                    resolution[eui] = port_address
                    self.runtime_state_info.runtime_state.set(eui, port_address)

                    node_class = type(node).__name__
                    if node_class not in node_schemas:
                        full_path_in = os.path.join(
                            os.path.join(self.cwd, "entropynodes"), "schema"
                        )
                        full_path_in = os.path.join(full_path_in, f"{node_class}.json")
                        with open(full_path_in, encoding="utf-8", mode="r") as f:
                            schema = json.loads(f.read())
                        node_schemas[node_class] = schema
                _Config.node_status_dict[node_name] = "resolved"
        logger.debug(
            f"Execute. Assign relative outputs to free ports."
            f"Node count: {total_node_count}; "
            f"Node schemas: {node_schemas}."
        )
        return total_node_count, node_schemas

    def __write_parameter_resolution_in_the_playbook(self):
        logger.debug("Execute. Write parameter resolution in the playbook.")
        for node_name, node in self.workflow._nodes.items():
            self.runtime_state_info.runtime_state.set(
                f"#{node_name}", node._inputs._inputs.print_defined()
            )

    def __start_execution_if_ready(self, ready_nodes_count, total_node_count):
        logger.debug(
            f"Execute. Start execution if ready. "
            f"Ready nodes count: {ready_nodes_count}. "
            f"Nodes count: {total_node_count}."
        )
        if ready_nodes_count == total_node_count:
            # tell nodes to stop sending on outputs and wait buffer flush
            self.executor_output.send(msgpack.packb({"cmd": "wait_flush"}))

            # wait for all nodes to stop
            self.__zmq_messages_status_check(
                total_node_count,
                self.args.connection_wait,
                "waiting_flush",
            )

            # flush buffer outputs
            self.executor_output.send(msgpack.packb({"cmd": "flush"}))

            # wait for all nodes to flush outputs
            self.__zmq_messages_status_check(
                total_node_count,
                self.args.connection_wait,
                "ready",
            )

            # start execution
            execution_connection_problem = False
            self.executor_output.send(msgpack.packb({"cmd": "start"}))
        else:
            execution_connection_problem = True
            _Config.execution_active = False
        logger.debug(
            f"Execute. Start execution if ready. "
            f"Execution connection problem: {execution_connection_problem}."
        )
        return execution_connection_problem

    def __wait_for_finish_or_terminate(self):
        # wait for finish, if debug on periodically snapshot process performance
        # also listen to external signal (in case the execution termination request)
        # is received
        logger.debug("Execute. Wait for finish or terminate.")
        max_time = self.args.max_execution_time
        active_nodes = len(self.processes_list)
        time_step = self.args.status_check_interval

        while (
            _Config.execution_active
            and (max_time > 0 or (self.args.max_execution_time == 0))
            and active_nodes > 0
        ):
            gone, alive = self.__wait_for_processes(
                [],
                "success",
                "finished",
                wait_process_timeout=time_step,
            )
            active_nodes = len(alive)
            execute_utils.check_node_messages(self.executor_input)
            max_time -= time_step
        execution_timeout_event = not (
            max_time > 0 or (self.args.max_execution_time == 0)
        )
        logger.debug(
            f"Execute. Wait for finish or terminate."
            f"Execution timeout event: {execution_timeout_event}"
        )
        return execution_timeout_event

    def __terminate(self):
        logger.debug("Execute. Terminate.")
        # send terminate signal to all the nodes
        processListAll = []
        for p in self.processes_list:
            for child in p.children(recursive=True):
                processListAll.append(child)
        for p in processListAll:
            p.terminate()

        # wait for nodes to gracefully exit
        logger.debug("Execute. Terminate. Wait for nodes to gracefully exit. v0")
        gone, alive = self.__wait_for_processes(processListAll, "success", "finished")

        # kill all the remaining nodes
        logger.debug("Execute. Terminate. Kill all the remaining nodes. v0")
        for p in alive:
            if p not in self.processes_list:
                # kill only executed process, not the shell script itself
                # in order to keep log file
                p.kill()

        logger.debug("Execute. Terminate. Wait for finish. v1")
        gone, alive2 = self.__wait_for_processes(alive, "", "finished")

        logger.debug("Execute. Terminate. Kill all the remaining nodes. v1")
        for p in self.processes_list:
            # kill even shell script itself if it is still active
            p.kill()

        logger.debug("Execute. Terminate. Wait for finish. v2")
        self.__wait_for_processes(alive2, "success", "finished")

    def __flush_runtimedata_into_hdf5(self):
        # add def for decrease tab level
        def _update_node_output(db, node_name, o, node_info, grp):
            eui = f"#{node_name}/{o}"

            if node_info["outputs"][0]["retention"][o] == 2:
                results = (
                    db.execute(sql_text(f'SELECT value FROM "{eui}"')).scalars().all()
                )
                results_time = (
                    db.execute(
                        sql_text(
                            f"""
        SELECT to_char
        (time::timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
        FROM "{eui}"
        """
                        )
                    )
                    .scalars()
                    .all()
                )
                dset = grp.create_dataset(f"{o}_time", data=results_time)
                try:
                    dset = grp.create_dataset(o, data=results)
                except TypeError:
                    r = []
                    for elem in results:
                        r.append(json.dumps(elem))
                    dset = grp.create_dataset(o, data=r)
                dset.attrs["description"] = node_info["outputs"][0]["description"][o]
                dset.attrs["units"] = node_info["outputs"][0]["units"][o]

        # add def for decrease tab level
        def _update_node(db, node_name, node, node_info):
            # node_info = self.workflow._node_details(node)
            grp = f.create_group(f"{node_name}")
            grp.attrs["type"] = node_info["name"]
            grp.attrs["description"] = node_info["description"]
            grp.attrs["bin"] = node_info["bin"]

            for o in node._outputs._outputs:
                _update_node_output(db, node_name, o, node_info, grp)

        # Flush the runtimedata into data_store HDF5 file
        logger.debug("Execute. Flush runtime data into hdf5.")
        with execute_utils.get_runtimedata(_Config.DATABASE_NAME) as db:
            with h5py.File(f"./{_Config.job_id}.hdf5", "w") as f:
                execute_utils.write_metadata_to_h5(f, self.metadata)
                for node_name, node in self.workflow._nodes.items():
                    node_info = self.workflow._node_details(node)
                    _update_node(db, node_name, node, node_info)

    def __clean_the_playbook(self):
        logger.debug("Execute. Clean the playbook.")
        runtime_state = self.runtime_state_info.runtime_state
        for node_name, node in self.workflow._nodes.items():
            runtime_state.delete(f"pid#{node_name}")
            for o in node._outputs._outputs:
                eui = f"#{node_name}/{o}"
                port_number = runtime_state.get(eui).decode().split(":")[-1]
                runtime_state.delete(f"system/port{port_number}")
                runtime_state.delete(eui)

            runtime_state.delete(f"#{node_name}")

    def __clean(self):
        logger.debug("Execute. Clean.")
        self.runtime_state_info.runtime_state.delete("dataserver")

        if self.message_queue_info.updates_channel is not None:
            self.message_queue_info.status_connection.close()

        self.runtime_state_info.runtime_state.delete("executor_output")
        self.runtime_state_info.runtime_state.delete("executor_input")
        self.executor_input.close()
        self.executor_output.close()
        execute_utils.remove_port_lock(
            self.port_input, self.runtime_state_info.runtime_state
        )
        execute_utils.remove_port_lock(
            self.port_output, self.runtime_state_info.runtime_state
        )

        self.runtime_state_info.runtime_state.delete("executor_pid")

    def __result_message(self, execution_timout_event, execution_connection_problem):
        logger.debug("Execute. Result message.")
        status_message = ""
        if execution_timout_event:
            status_message = "Execution timed out."
        if execution_connection_problem:
            status_message = (
                "Connection between nodes timed out before being established."
            )

        nodes_success = 0
        nodes_error = 0

        for _node, value in _Config.node_status_dict.items():
            if value != "success":
                nodes_error += 1
            else:
                nodes_success += 1
        if nodes_error == len(_Config.node_status_dict):
            status = "Failure"
        elif status_message == "" and nodes_error == 0:
            status = "Success"
        else:  # previous condition: nodes_error > 0.
            status = "Partially successful"

        return json.dumps(
            {
                "status": status,
                "status_message": status_message,
                "nodes_success": nodes_success,
                "nodes_error": nodes_error,
                "nodes_count": len(_Config.node_status_dict),
                "nodes": _Config.node_status_dict,
            }
        )

    def run(self):
        self.__setup_executor_input_output(_Config.port_number)
        total_node_count, node_schemas = self.__assign_relative_outputs_to_free_ports()
        self.__write_parameter_resolution_in_the_playbook()
        self.runtime_state_info.runtime_state.set("executor_pid", os.getpid())

        # initialize all the nodes, saving process id
        self.__init_nodes(node_schemas)

        # make sure all requested nodes are up before giving the green light for
        # them to start communicating
        ready_nodes_count = self.__zmq_messages_status_check(
            total_node_count,
            self.args.connection_wait,
            "connected",
            True,
        )

        execution_connection_problem = self.__start_execution_if_ready(
            ready_nodes_count, total_node_count
        )
        execution_timeout_event = self.__wait_for_finish_or_terminate()
        self.__terminate()
        # clean the playbook
        self.__clean_the_playbook()
        self.__flush_runtimedata_into_hdf5()
        # clean playbook reference to data storage location
        self.__clean()
        result_message = self.__result_message(
            execution_timeout_event, execution_connection_problem
        )
        logger.debug(f"Execute. Run. Result: {result_message}")
        return result_message

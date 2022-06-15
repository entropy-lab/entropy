import zmq
import msgpack
import datetime
from . import nodeio_context
import psycopg2

__all__ = ["Outputs"]


class Outputs:

    __local_instances = []
    __db_write = None
    __bucket = None

    @classmethod
    def _register(cls, instance):
        cls.__local_instances.append(instance)
        return len(cls.__local_instances) - 1

    @classmethod
    def _schema(cls):
        outputList = []
        for v in cls.__local_instances:
            outputList.append(
                {
                    "description": v.description,
                    "units": v.units,
                    "retention": v.retention,
                }
            )
        return outputList

    @classmethod
    def _connections(cls):
        return cls.__local_instances[0].connections

    def __init__(self):
        self.__index = self._register(self)
        self.description = {}
        self.units = {}
        self.retention = {}
        self.values = {}
        self.connections = {}
        self.db_write_api = None

    def define(self, name, units="", description="", retention=0):
        if name.find(" ") != -1 or name.find("/") != -1:
            raise ValueError(
                "Input name cannot contain spaces or / characters. "
                "Use underscore_names or camelCase."
            )
        self.description[name] = description
        self.units[name] = units
        self.retention[name] = retention
        self.values[name] = []
        # open local communication if part of workflow
        if nodeio_context.playbook is not None:
            my_output = nodeio_context.playbook.get(
                f"#{nodeio_context.entropy_identity}/{name}"
            ).decode()
            context = nodeio_context.zmq_context()
            socket = context.socket(zmq.PUB)
            socket.setsockopt(zmq.LINGER, 0)
            socket.bind(my_output)
            self.connections[name] = socket

    def set(self, **kwargs):
        """
        Simple setting of output, that accepts multiple variables at the same time.

        Depending on the retention specified when output variable was defined
        this value will just be available to other nodes, or it will also be
        saved to the database.
        """
        for key, value in kwargs.items():
            assert key in self.values, (
                f"There is no field name '{key}' in Output fields.\n"
                f"All output field names have to be defined at the start."
            )
            # write internally

            encoded_value = msgpack.packb(value)

            if key in self.connections:
                self.connections[key].send(encoded_value)

            if nodeio_context.runtime_data is not None:
                if self.retention[key] > 0:
                    # if retention is non-zero send to database

                    # TODO: use zeromq bridge to SQL to simplify nodeio logic
                    # and have one place to control reliability
                    nodeio_context.runtime_data.execute(
                        f"""INSERT INTO "#{nodeio_context.entropy_identity}/{key}" """
                        f""" (time, value) VALUES (NOW(), msgpack_decode(%s));
                            """,
                        (psycopg2.Binary(encoded_value)),
                    )
            else:
                # for debugging purposes
                print(
                    f"\tENTROPYLAB - OUTPUT #{nodeio_context.node_name}/{key}"
                    f" SAVE (retention = {self.retention[key]})"
                )
                print(f"\t{value}")
                if nodeio_context.save_dry_run and self.retention[key] == 2:
                    if key not in nodeio_context.dry_run_data["node"]["outputs"]:
                        output = {}
                        output["values"] = []
                        output["values_time"] = []
                        output["description"] = self.description[key]
                        output["units"] = self.units[key]
                        nodeio_context.dry_run_data["node"]["outputs"][key] = output

                    nodeio_context.dry_run_data["node"]["outputs"][key][
                        "values"
                    ].append(value)
                    nodeio_context.dry_run_data["node"]["outputs"][key][
                        "values_time"
                    ].append(datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))

        return

    # TODO: add also write that will have more controllable options when writing data

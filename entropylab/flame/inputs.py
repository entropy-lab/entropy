from .enum_types import InputType, ioFormat
from . import nodeio_context
import json
import zmq
import msgpack
import time

__all__ = ["Inputs"]


class Inputs:

    __local_instances = []  # where to get values from when used in stand-alone mode
    __external_intances = []  # where to get values when used as a module

    @classmethod
    def _register(cls, instance):
        cls.__local_instances.append(instance)
        return len(cls.__local_instances) - 1

    @classmethod
    def _schema(cls):
        inputList = []
        for v in cls.__local_instances:
            # TODO: handle properly Inputs instances in Inputs lists
            typeDict = {}
            for k, e in v.input_type.items():
                typeDict[k] = e.value
            inputList.append(
                {"description": v.description, "units": v.units, "type": typeDict}
            )
        return inputList

    @classmethod
    def _set_as_external_input(cls, instance):
        cls.__local_instances.remove(instance)
        cls.__external_intances.append(instance)
        instance.__index = len(cls.__external_intances) - 1

    @classmethod
    def _connections(cls):
        return cls.__local_instances[0].connections

    def __init__(self):
        self.__index = self._register(self)
        if self.__index < len(self.__external_intances):
            # overwrite inputs with externally imposed inputs
            self.values = self.__external_intances[self.__index].values
            self.units = self.__external_intances[self.__index].units
            self.value_set = self.__external_intances[self.__index].value_set
            self.description = self.__external_intances[self.__index].description
            self.input_type = self.__external_intances[self.__index].input_type
        else:
            # use locally defined inputs
            self.values = {}
            self.units = {}
            self.value_set = {}
            self.description = {}
            self.input_type = {}
        self.connections = {}  # communication middle ware providing inputs
        self.convert_to = {}  # data type conversion from plain text
        self.last_change = {}  # since the last reading, or None if no new updates

    def state(self, name: str, description="", units=""):
        """Input that holds only last received value. It is not consumed on
        reading. It doesn't block if it has been set in the past. Useful for
        stateful variables.
        """
        return self.__input(name, InputType.STATE, description=description, units=units)

    def stream(self, name: str, description="", units=""):
        """Input that accumulates received values for consumption (by get method),
        It blocks on read if until there are elements to be consumed. Useful for
        data streams and events.
        """
        return self.__input(
            name, InputType.STREAM, description=description, units=units
        )

    def __input(self, name: str, input_type: InputType, description="", units=""):
        """Returns a function whose value can be obtained later to obtain
        input value.
        """

        if name.find(" ") != -1 or name.find("/") != -1:
            raise ValueError(
                "Input name cannot contain spaces or / characters. "
                "Use underscore_names or camelCase."
            )
        if self.__index >= len(self.__external_intances):
            # if we don't have external inputs, initialize values
            if name in self.values:
                raise ValueError(f"Input varible {name} has been alredy defined.")
            self.values[name] = None
            self.value_set[name] = False
        else:
            # we have external inputs
            if (
                type(self.values[name][0]) == str
                and len(self.values[name][0]) > 0
                and self.values[name][0][0] == "#"
            ):
                # if they are runtime variables, initialize connections
                self.value_set[name] = False
                context = nodeio_context.zmq_context()
                socket = context.socket(zmq.SUB)
                if input_type == InputType.STATE:
                    # state variables should get only the last, up-to date
                    # variable
                    socket.setsockopt(zmq.CONFLATE, 1)
                socket.setsockopt(zmq.LINGER, 0)
                socket.connect(self.values[name][0][1:])
                socket.subscribe("")
                self.connections[name] = socket

        self.description[name] = description
        self.units[name] = units
        self.input_type[name] = input_type
        return lambda: self.get(name)

    def get(self, name: str):
        """Returns value of the input

        For state variables, returns currently value, blocks only if it
        has not been still resolved at all (first resolution).

        For stream variables, returns current value, blocking if no value is
        available until value is received. If module is used in stand-alone
        mode, instead of blocking, once the input values are all used,
        it will throw error.
        """
        if self.__index >= len(self.__external_intances):
            assert self.value_set[name], f"Inputs '{name}' has not been defined yet."
            if len(self.values[name]) == 0:
                print(
                    "\tENTROPYLAB - INPUTS: End of the input queue "
                    f"{nodeio_context.node_name}/{name} is reached, "
                    "terminating test run of the node."
                )
                nodeio_context.terminate_node()
            if self.input_type[name] == InputType.STREAM:
                print(f"\tENTROPYLAB - STREAM INPUT {nodeio_context.node_name}/{name}")
            else:
                print(f"\tENTROPYLAB - STATE INPUT {nodeio_context.node_name}/{name}")
            if len(self.values[name]) == 1 and self.input_type[name] == InputType.STATE:
                # InputType.STATE
                # we don't have sequence of states to test
                # assume that last state stays from now on
                value = self.values[name][0]
            else:
                value = self.values[name].pop(0)

            print(f"\t{value}")
            return value

        else:
            # node is part of workflow
            if name in self.connections:
                # requested input is runtime variable
                if self.input_type[name] == InputType.STREAM:
                    if self.value_set[name]:
                        # there is cached value
                        self.value_set[name] = False
                        return self.values[name]

                    # else: block until input is resolved
                    try:
                        self.values[name] = self.convert_to[name](
                            msgpack.unpackb(self.connections[name].recv())
                        )
                    except Exception as e:
                        print(repr(e))
                        exit(1)
                    return self.values[name]
                elif self.input_type[name] == InputType.STATE:
                    if self.value_set[name]:
                        # we already have some state runtime variable
                        # just check in non-blocking manner if there is update
                        # to this value
                        try:
                            # check for updates
                            self.values[name] = self.convert_to[name](
                                msgpack.unpackb(
                                    self.connections[name].recv(flags=zmq.NOBLOCK)
                                )
                            )
                        except zmq.error.ZMQError as e:
                            if e.errno == zmq.EAGAIN:
                                # the state value has not been updated since last
                                # get call
                                self.last_change[name] = None
                                return self.values[name]
                            else:
                                raise Exception(repr(e))
                        self.last_change[name] = None
                        return self.values[name]
                    else:
                        # this is first resolution of the state runtime variable
                        # block here until the value is received
                        try:
                            self.values[name] = self.convert_to[name](
                                msgpack.unpackb(self.connections[name].recv())
                            )
                        except Exception as e:
                            print(repr(e))
                            exit(1)
                        self.last_change[name] = None
                        self.value_set[name] = True
                        return self.values[name]

            # requested input is parameter
            return self.convert_to[name](self.values[name][0])

    def set(self, **kwargs):
        """Sets the value of the input.

        State variables will save only the latest set value.
        Stream varialbes will save all the variables in a queue and make them
        available for consummers.
        """
        for key, value in kwargs.items():
            if key not in self.values:
                raise ValueError(f"The input {key} has not been defined.")
            # deduce type for conversion
            if value is not None:
                self.convert_to[key] = type(value)
            else:
                self.values[key] = None
                self.value_set[key] = False
                return
            # if we are using external instance of inputs prevent overwritting of the parameters
            if self.__index >= len(self.__external_intances):
                if self.values[key] is None:
                    self.values[key] = []

                self.values[key].append(value)

                self.value_set[key] = True
        return

    def updated(self, name):
        """Checks if value has been updated.

        Note that this does not check the variable value, just if the variable
        has been set again since the last time we get it.

        :returns: bool
        """
        if self.input_type[name] == InputType.STATE:
            if name not in self.connections:
                # this is either a parameter (not a runtime variable)
                return False
            # else this is a run-time variable that has not been yet resolved
            if name in self.last_change and self.last_change[name] is not None:
                return True
            # check for updates
            try:
                self.values[name] = self.convert_to[name](
                    msgpack.unpackb(self.connections[name].recv(flags=zmq.NOBLOCK))
                )
                self.last_change[name] = time.time()
            except zmq.error.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    # the state value has not been updated since last
                    # get call
                    return False
                else:
                    raise Exception(repr(e))
            return True
        else:
            # case when self.input_type[name] == InputType.STREAM
            if self.value_set[name]:
                # there is unconsumed stream value
                return True
            else:
                # check if there is update
                try:
                    self.values[name] = self.convert_to[name](
                        msgpack.unpackb(self.connections[name].recv(flags=zmq.NOBLOCK))
                    )
                    self.value_set[name] = True
                    return True
                except zmq.error.ZMQError as e:
                    if e.errno == zmq.EAGAIN:
                        # this value has not been set yet
                        return False
                    else:
                        raise Exception(repr(e))

    def remove(self, name):
        """Removes named input

        Values of this input will be deleted and this named input will be
        removed from list of available inputs.
        Note that any get calls to this name will be undefined, unless input
        with this name is added at later time.
        """
        if name in self.values:
            del self.values[name]
            del self.value_set[name]
            del self.description[name]
            del self.units[name]
            del self.input_type[name]

    def reset(self, name):
        """Resets input value to not set, keeping the input variable"""
        if name in self.values:
            self.values[name] = None
            self.value_set[name] = False

    def reset_all(self):
        """Resets all set input values to not set, keeping the input variable."""
        if nodeio_context.entropy_identity is None:
            return
        for name, _value in self.values.items():
            self.values[name] = None
            self.value_set[name] = False

    def reset_all_dry_run_data(self):
        """Resets dry-run data only, keeping inputs untouched if they are part
        of the workflow. Useful when authoring node interactively in Jupyter
        nodebook, when set might be called repeatedly by executing corresponding
        Jupyter notebook cell."""
        if nodeio_context.entropy_identity is None:
            return
        else:
            self.reset_all()

    def defines(self, name):
        """Returns true if input exists and it is defined."""
        return (name in self.values) and (self.value_set[name])

    def completely_defined(self):
        return self.get_as_JSON("undefined") == {}

    def print_all(self, indent=2, sort_keys=True):
        inputs = self.get_as_JSON("all")
        return json.dumps(inputs, indent=indent, sort_keys=sort_keys)

    def print_defined(self, indent=2, sort_keys=True):
        inputs = self.get_as_JSON("defined")
        return json.dumps(inputs, indent=indent, sort_keys=sort_keys)

    def print_undefined(self, indent=2, sort_keys=True):
        inputs = self.get_as_JSON("undefined")
        return json.dumps(inputs, indent=indent, sort_keys=sort_keys)

    def get_as_JSON(self, what="all", format=ioFormat.VALUES):
        def _filter_all(value_set):
            return True

        def _filter_defined(value_set):
            return value_set

        def _filter_undefined(value_set):
            return not value_set

        if what == "all":
            filter = _filter_all
        elif what == "defined":
            filter = _filter_defined
        elif what == "undefined":
            filter = _filter_undefined
        else:
            raise ValueError(
                "Only all, defined and undefined are valid inputs " "for get_as_JSON"
            )

        outJSON = {}
        for k, v in self.values.items():
            if self.value_set[k]:
                v = v[0]  # only current value if we have event sequence
            if filter(self.value_set[k]):
                if isinstance(v, Inputs):
                    outJSON[k] = v.get_as_JSON(what=what, format=format)
                elif isinstance(v, list):
                    outJSON[k] = []
                    for element in v:
                        if isinstance(element, Inputs):
                            outJSON[k].append(
                                element.get_as_JSON(what=what, format=format),
                            )
                        else:
                            outJSON[k].append(element)
                else:
                    outJSON[k] = v
        return outJSON

    def load_JSON(self):
        pass

    def view_as_HTML(self, enrich=True):
        """If enrich is True, add docstrings"""
        pass

    def print_docstring(self, name):

        pass

    def __repr__(self) -> str:
        return "Inputs"

    def print_instances(self):
        print(self.__local_instances)

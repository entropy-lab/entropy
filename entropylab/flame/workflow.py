import atexit
import os
import json
import networkx as nx
from .enum_types import ioFormat

__all__ = ["Workflow"]


class BagOfNodes(object):
    def __init__(self):
        pass

    def __repr__(self) -> str:
        """node list"""
        node_list = ""
        for a in dir(self):
            if not a.startswith("__"):
                node_list += f"\n{a}"
        return node_list


class NodeGroup(object):
    def __init__(self, name):
        self.name = name
        self.nodes = []

    def add(self, node):
        for x in self.nodes:
            if x._name == node._name:
                raise ValueError(
                    f"Node '{node._name}' is already added to group '{self.name}'"
                )
        self.nodes.append(node)


class Workflow(object):

    __workflows = []
    __current_workflow_context = None

    @classmethod
    def _register_node(cls, node_instance, prepend_name=""):
        if cls.__current_workflow_context is None:
            raise ValueError(
                "Before creating instances of nodes you have"
                " to define Workflow instance"
            )
        node_list = cls.__workflows[cls.__current_workflow_context]._nodes
        node_name = prepend_name + node_instance._name
        if node_name in node_list:
            raise ValueError(
                ("Node with Entropy Unique Identifier '%s'" % node_name)
                + " already exists!\nPlease make sure node names are unique.\n"
                + "Aborting workflow creation"
            )
        node_list[node_name] = node_instance
        setattr(
            cls.__workflows[cls.__current_workflow_context].n, node_name, node_instance
        )

    @classmethod
    def _add_to_workflow_list(cls, workflow_instance):
        cls.__workflows.append(workflow_instance)
        atexit.register(workflow_instance._generate_parameter_file)
        return len(cls.__workflows) - 1

    @classmethod
    def _main_workflow(cls):
        if cls.__current_workflow_context is None:
            return None
        return cls.__workflows[cls.__current_workflow_context]

    @classmethod
    def _main_workflow_index(cls):
        return cls.__current_workflow_context

    @classmethod
    def _register_main_workflow(cls, workflow_instance):
        cls.__current_workflow_context = workflow_instance._index

    @classmethod
    def _prevent_parameter_file_overwrite(cls):
        cls.__current_workflow_context = None

    def __init__(self, name, description=""):
        """New workflow

        :param name: name of workflow
        :param description: longer description of workflow
        """
        self._nodes = {}
        self.name = name
        self.description = description
        #: """ Access nodes by names as workflow.n.<node_name> """
        self.n = BagOfNodes()
        self._index = self._add_to_workflow_list(self)
        self._views = {}
        self._node_info = {}  #: type of nodes used
        self.register()

    def register(self):
        """
        When workflow is registred, if workflow file is executed, the
        workflow will generate parameter list. This workflow file can also
        then be used for executor
        """
        self._register_main_workflow(self)

    def _generate_parameter_file(self):
        if self._index != self._main_workflow_index():
            return
        full_path_out = os.path.join(os.getcwd(), "parameters.json")
        existing_parameters = None
        if os.path.exists(full_path_out):
            # load existing file
            try:
                with open(full_path_out, "r") as file:
                    existing_parameters = json.load(file)
            except Exception:
                pass
        with open(full_path_out, encoding="utf-8", mode="w") as f:
            f.write(self._list_missing_parameters())

        if existing_parameters is not None:
            # use previously existing parameters.json to resolve values
            try:
                reused_values = 0
                with open(full_path_out, "r") as file:
                    parameters = json.load(file)
                for node, values in existing_parameters.items():
                    if node in parameters:
                        for key, value in values.items():
                            if key in parameters[node]:
                                if value is not None:
                                    parameters[node][key] = value
                                    reused_values += 1
                if reused_values > 0:
                    with open(full_path_out, encoding="utf-8", mode="w") as f:
                        f.write(json.dumps(parameters, indent=2))
                    manyvalues = "s" if reused_values > 1 else ""
                    print(
                        f"Reused {reused_values} parameter value{manyvalues} "
                        "from existing parameters.json file."
                    )
            except Exception:
                pass

    def _resolve_parameters(self, json_filename):
        with open(json_filename, "r") as file:
            parameters = json.load(file)

        for node, node_param in parameters.items():
            self._nodes[node]._inputs._inputs.set(**node_param)
        for node_name, node in self._nodes.items():
            if not node._inputs._inputs.completely_defined():
                print(f"\tNode '{node_name}' parameters are not completely resolved!")
                print("\tMissing parameters:")
                print(self._list_missing_parameters())
                exit()
                # raise ValueError("Execution cannot run before all parameters are resolved.")

    def _list_missing_parameters(self, existing_parameters=None):
        parameter_listing = "{\n"
        unresolved_nodes = 0
        for name, node in self._nodes.items():
            if not node._inputs._inputs.completely_defined():
                if unresolved_nodes != 0:
                    parameter_listing += ",\n"
                parameter_listing += (
                    f'"{name}":' + node._inputs._inputs.print_undefined()
                )
                unresolved_nodes += 1
        parameter_listing += "\n}"
        return parameter_listing

    def _resolved_parameters(self):
        parameters = {}
        for name, node in self._nodes.items():
            parameters[name] = node._inputs._inputs.get_as_JSON(
                what="defined", format=ioFormat.VALUES
            )
        return parameters

    def _to_json(self):
        """Export as JSON with Cytoscape graph representation"""
        graph = nx.DiGraph()
        icons = {}
        # add nodes
        for node_name, node in self._nodes.items():
            graph.add_node(node_name, ident=node_name, name=type(node).__name__)
            icons[type(node).__name__] = node._icon
        # add links
        for node_name, node in self._nodes.items():
            for _input_name, input_value in node._inputs._inputs.values.items():
                if (
                    input_value is not None
                    and type(input_value[0]) == str
                    and len(input_value[0]) > 2
                    and input_value[0][0] == "#"
                    and input_value[0][1] != "/"
                ):
                    # relative EUI reference to runtime variable
                    source_node = input_value[0][1:].split("/")[0]
                    graph.add_edge(source_node, node_name)

        result = {
            "name": self.name,
            "description": self.description,
            "graph": nx.cytoscape_data(graph),
            "resolved_inputs": self._resolved_parameters(),
            "icons": icons,
        }

        return json.dumps(result)

    def _node_details(self, node):
        node_class = type(node).__name__
        if node_class not in self._node_info:
            json_filename = os.path.join(
                os.getcwd(), "entropynodes", "schema", f"{node_class}.json"
            )
            with open(json_filename, "r") as file:
                self._node_info[node_class] = json.load(file)
        return self._node_info[node_class]

    def _summary_json(self):
        """Export as JSON summary of indexable information about workflow"""

        used_node_types = {}
        for _, node in self._nodes.items():
            node_info = self._node_details(node)
            node_class = type(node).__name__

            inputs = {}
            for input_name, input in node_info["inputs"][0]["description"].items():
                units = node_info["inputs"][0]["units"][input_name]
                inputs[input_name + f" ({units})"] = input
            outputs = {}
            for output_name, output in node_info["outputs"][0]["description"].items():
                units = node_info["outputs"][0]["units"][output_name]
                outputs[output_name + f" ({units})"] = output
            outputs_retention = {}
            for output_name, retention in node_info["outputs"][0]["retention"].items():
                outputs_retention[output_name] = retention
            used_node_types[node_class] = {
                "description": node_info["description"],
                "inputs": inputs,
                "outputs": outputs,
                "outputs_retention": outputs_retention,
            }

        result = {
            "name": self.name,
            "description": self.description,
            "nodes": used_node_types,
        }

        return json.dumps(result)

    def add(self, node_or_workflow, prepend_name=""):
        if isinstance(node_or_workflow, Workflow):
            for _name, node in node_or_workflow._nodes.items():
                self._register_node(node, prepend_name=prepend_name)
        else:
            self._register_node(node_or_workflow)

    def parameters(self):
        pass

    def group(self, node_list, group_name, view="default"):
        pass

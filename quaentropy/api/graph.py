from __future__ import annotations

from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Set, Dict, Any, List, Optional

import networkx as nx
from graphviz import Digraph

from quaentropy.api.execution import EntropyContext


@dataclass(frozen=True, eq=True)
class Output:
    node: Node
    name: str


class Node(ABC):
    def __init__(
        self,
        label: str = None,
        input_vars: Dict[str, Output] = None,
        output_vars: Set[str] = None,
        must_run_after: Set[Node] = None,
    ):
        self._label = label
        self._input_vars = input_vars
        if self._input_vars is None:
            self._input_vars = {}
        self._output_vars: Set[str] = output_vars
        if self._output_vars is None:
            self._output_vars = {}
        self._must_run_after: Set[Node] = must_run_after
        if self._must_run_after is None:
            self._must_run_after = {}

    @property
    def label(self) -> str:
        return self._label

    @property
    def outputs(self) -> Dict[str, Output]:
        return {
            output_name: Output(self, output_name) for output_name in self._output_vars
        }

    def add_input(self, name: str, parent_node_output: Output):
        if name in self._input_vars:
            raise KeyError(f"Input {name} already exist")

        self._input_vars[name] = parent_node_output

    def get_parents(self) -> List[Node]:
        return list(set([var.node for var in self._input_vars.values()])) + list(
            self._must_run_after
        )

    def get_inputs(self) -> List[Output]:
        return [var for var in self._input_vars.values()]

    @abstractmethod
    async def execute_async(
        self,
        parents_results: List[Dict[str, Any]],
        context: EntropyContext,
        node_execution_id: int,
        is_last,
        **kwargs,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def execute(
        self,
        parents_results: List[Dict[Output, Any]],
        context: EntropyContext,
        node_execution_id: int,
        is_last,
        **kwargs,
    ) -> Dict[str, Any]:
        pass

    def ancestors_set(self) -> Set[Node]:
        parents: Set[Node] = set([var.node for var in self._input_vars.values()])
        ancestors = parents.copy()
        ancestors.add(self)
        for anc in parents:
            ancestors.update(anc.ancestors_set())
        return ancestors


class Graph:
    def __init__(
        self,
        nodes: Set[Node],
        label: str = None,
        plot_outputs: Optional[Set[str]] = None,
    ) -> None:
        super().__init__()
        self.label: str = label
        self._nodes: Set[Node] = nodes
        self._end_nodes = self._calculate_last_nodes(nodes)
        self._plot_outputs = plot_outputs
        if self._plot_outputs is None:
            self._plot_outputs = set()

    def _calculate_last_nodes(self, nodes):
        end_nodes = nodes.copy()
        for node in nodes:
            for parent in node.get_parents():
                if parent in end_nodes:
                    end_nodes.remove(parent)
        return end_nodes

    @property
    def nodes(self) -> Set[Node]:
        return self._nodes

    def end_nodes(self) -> List[Node]:
        return self._end_nodes

    @property
    def plot_outputs(self) -> Set[str]:
        return self._plot_outputs

    def export_dot_graph(self) -> Digraph:
        """
        Converts the graph into DOT graph format
        :return: str
        """
        visited_labels = {}
        node_labels = {}

        def unique_label(node):
            if node in node_labels:
                return node_labels[node]
            if node.label in visited_labels:
                index = visited_labels[node.label] + 1
                visited_labels[node.label] = index
                node_labels[node] = f"{node.label}_{str(index)}"
            else:
                visited_labels[node.label] = 1
                node_labels[node] = node.label
            return node_labels[node]

        dot = Digraph(comment=self.label)
        for node in self._nodes:
            dot.node(unique_label(node), shape="box")

        for node in self._nodes:
            parents = node.get_parents()
            inputs = node.get_inputs()
            for parent in parents:
                input_names = [
                    input.name
                    for input in filter(lambda input: input.node == parent, inputs)
                ]
                dot.edge(
                    unique_label(parent), unique_label(node), ",".join(input_names)
                )

        return dot
        #
        # g = nx.DiGraph()
        #
        # def unique_label(node):
        #     return f"{node.label} at {id(node)}"
        #
        # for node in self.nodes:
        #     g.add_node(unique_label(node))
        #
        # for node in self.nodes:
        #     for parent in node.get_parents():
        #         g.add_edge(unique_label(parent), unique_label(node))
        # dot = nx.nx_pydot.to_pydot(g)
        # return dot

    def nodes_in_topological_order(self):
        g = nx.DiGraph()

        for node in self.nodes:
            g.add_node(node)

        for node in self.nodes:
            for parent in node.get_parents():
                g.add_edge(parent, node)

        sorted_list = nx.topological_sort(g)

        return list(sorted_list)

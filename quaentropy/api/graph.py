from __future__ import annotations

from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Set, Dict, Any, List, Optional

from graphviz import Digraph

from quaentropy.api.execution import EntropyContext


@dataclass
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

    def run(self):
        # todo guy
        pass

    @abstractmethod
    async def execute(
        self,
        parents_results: List[Dict[str, Any]],
        context: EntropyContext,
        node_execution_id: int,
        depth_from_last,
        **kwargs,
    ) -> Dict[str, Any]:
        pass


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
        self._end_nodes = self._calculate_end_nodes(nodes)
        self._plot_outputs = plot_outputs
        if self._plot_outputs is None:
            self._plot_outputs = set()

    def _calculate_end_nodes(self, nodes):
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
        dot = Digraph(comment=self.label)
        for node in self._nodes:
            dot.node(node._label, shape="box")

        for node in self._nodes:
            parents = node.get_parents()
            inputs = node.get_inputs()
            for parent in parents:
                input_names = [
                    input.name
                    for input in filter(lambda input: input.node == parent, inputs)
                ]
                dot.edge(parent._label, node._label, ",".join(input_names))

        return dot

import asyncio
import sys
import traceback
from datetime import datetime
from inspect import signature, iscoroutinefunction, getfullargspec
from typing import Optional, Dict, Any, List, Set, Union, Callable, Coroutine

import jsonpickle

from quaentropy.api.data_writer import (
    ExecutionSerializer,
    RawResultData,
    PlotDataType,
    Plot,
    DataWriter,
)
from quaentropy.api.errors import EntropyError
from quaentropy.api.execution import ExperimentExecutor, EntropyContext
from quaentropy.api.experiment import ExperimentDefinition
from quaentropy.api.graph import Graph, Node, Output
from quaentropy.api.plot import BokehCirclePlotGenerator
from quaentropy.instruments.lab_topology import LabTopology
from quaentropy.logger import logger


def pynode(
    label: str,
    input_vars: Dict[str, Output] = None,
    output_vars: Set[str] = None,
    must_run_after: Set[Node] = None,
):
    def decorate(fn):
        return PyNode(label, fn, input_vars, output_vars, must_run_after)

    return decorate


class PyNode(Node):
    def __init__(
        self,
        label: str = None,
        program: Union[Callable, Coroutine] = None,
        input_vars: Dict[str, Output] = None,
        output_vars: Set[str] = None,
        must_run_after: Set[Node] = None,
    ):
        super().__init__(label, input_vars, output_vars, must_run_after)
        self._program = program

    async def execute(
        self,
        parents_results: List[Dict[str, Any]],
        context: EntropyContext,
        node_execution_id: int,
        depth_from_last,
        **kwargs,
    ) -> Dict[str, Any]:
        function_parameters = {k: v for d in parents_results for k, v in d.items()}

        sig = signature(self._program)

        for param in sig.parameters:
            if sig.parameters[param].annotation is EntropyContext:
                function_parameters[param] = context
        if "depth_from_last" in sig.parameters:
            function_parameters["depth_from_last"] = depth_from_last

        (
            args,
            varargs,
            varkw,
            defaults,
            kwonlyargs,
            kwonlydefaults,
            annotations,
        ) = getfullargspec(self._program)

        keyword_function_parameters = {}
        for arg in args + kwonlyargs:
            if (
                arg not in function_parameters
                and arg not in kwargs
                and (
                    (defaults and arg not in defaults)
                    or (kwonlydefaults and arg not in kwonlydefaults)
                )
            ):
                logger.error(f"Error in node {self.label} - {arg} is not in parameters")
                raise KeyError(arg)
            if arg in function_parameters:
                keyword_function_parameters[arg] = function_parameters[arg]
            elif arg in kwargs:
                keyword_function_parameters[arg] = kwargs[arg]

        args_parameters = []
        if varargs is not None:
            for item in function_parameters:
                if item not in keyword_function_parameters:
                    args_parameters.append(function_parameters[item])

        try:
            if iscoroutinefunction(self._program):
                results = await self._program(
                    *args_parameters, **keyword_function_parameters
                )
            else:
                results = self._program(*args_parameters, **keyword_function_parameters)

            outputs = {}
            if isinstance(results, Dict):
                for var in self._output_vars:
                    try:
                        outputs[var] = results[var]
                    except KeyError:
                        logger.error(
                            f"WARNING Could not fetch variable '{var}' "
                            f"from the results of node <{self.label}>"
                        )
                        pass
            else:
                if results:
                    raise EntropyError(
                        f"node {self.label} result should be a "
                        f"dictionary but is {type(results)}"
                    )
            return outputs
        except BaseException as e:
            raise e


class SubGraphNode(Node):
    def __init__(
        self,
        graph: Graph,
        label: str = None,
        input_vars: Dict[str, Output] = None,
        output_vars: Set[str] = None,
        must_run_after: Set[Node] = None,
    ):
        super().__init__(label, input_vars, output_vars, must_run_after)
        self._graph = graph

    async def execute(
        self,
        parents_results: List[Dict[str, Any]],
        context: EntropyContext,
        node_execution_id: int,
        depth_from_last,
        **kwargs,
    ) -> Dict[str, Any]:
        return await _AsyncGraphExecutor(self._graph, **kwargs).execute_async(context)


class _NodeExecutor:
    def __init__(self, node: Node) -> None:
        super().__init__()
        self._node: Node = node
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
        self.result: Dict[str, Any] = {}
        self.to_run = True

    async def run_async(
        self,
        parents_results: List[Dict[str, Any]],
        context: EntropyContext,
        depth_from_last: int,
        **kwargs,
    ) -> Dict[str, Any]:
        if self.to_run:
            logger.info(
                f"Running node <{self._node.__class__.__name__}> {self._node.label}"
            )
            self._start_time = datetime.now()
            logger.debug(
                f"Saving metadata before running node "
                f"<{self._node.__class__.__name__}> {self._node.label}"
            )
            node_execution_id = id(self)
            self.result = await self._node.execute(
                parents_results, context, node_execution_id, depth_from_last, **kwargs
            )
            # logger fetching results
            for output_id in self.result:
                output = self.result[output_id]
                context.add_result(
                    RawResultData(
                        label=f"output_{output_id}",
                        data=output,
                        stage=node_execution_id,
                    )
                )
            self._end_time = datetime.now()
            logger.debug(
                f"Done running node <{self._node.__class__.__name__}> {self._node.label}"
            )
            return self.result


class _AsyncGraphExecutor(ExperimentExecutor):
    def __init__(self, graph: Graph, to_node: Optional[Node] = None, **kwargs) -> None:
        super().__init__()
        self._graph: Graph = graph
        if to_node is not None and to_node not in self._graph.nodes:
            raise KeyError("Not is not in graph")

        self._to_node: Optional[Node] = to_node
        self._node_kwargs = kwargs
        self._tasks: Dict[Node, Any] = dict()
        self._executors: Dict[Node, _NodeExecutor] = dict()
        self._results: Dict = dict()
        self._stopped = False

    def execute(self, context: EntropyContext) -> Any:
        async_result = asyncio.run(self.execute_async(context))
        return async_result

    @property
    def failed(self) -> bool:
        return self._stopped

    async def execute_async(self, context: EntropyContext):
        # traverse the graph and run the nodes
        if self._to_node:
            end_nodes = [self._to_node]
        else:
            end_nodes = self._graph.end_nodes()

        chains = []
        for node in end_nodes:
            chains.append(self._run_node_and_ancestors(node, context, 0))

        result = await asyncio.gather(*chains)
        result = [x for x in result if x is not None]
        combined_result = {}
        if result:
            combined_result = {k: v for d in result for k, v in d.items()}
            for output in self._graph.plot_outputs:
                if output in combined_result:
                    context.add_plot(
                        Plot(
                            label=f"graph result {output}",
                            data=combined_result[output],
                            data_type=PlotDataType.np_2d,
                            bokeh_generator=BokehCirclePlotGenerator(),
                        )
                    )
        return combined_result

    async def _run_node_and_ancestors(
        self, node: Node, context: EntropyContext, depth_from_last: int
    ):
        tasks = []
        for parent in node.get_parents():
            if parent not in self._tasks:
                task = self._run_node_and_ancestors(
                    parent, context, depth_from_last + 1
                )
                tasks.append(task)
                self._tasks[parent] = task
            else:
                tasks.append(self._tasks[parent])
        results = []
        if len(tasks) > 0:
            await asyncio.wait(tasks)
            if self._stopped:
                return None
            results = []
            for parent in node.get_inputs():
                if (
                    parent.node not in self._executors
                    or parent.name not in self._executors[parent.node].result
                ):
                    raise EntropyError(
                        f"node {node.label} input is missing: {parent.name}"
                    )
                results.append(
                    {parent.name: self._executors[parent.node].result[parent.name]}
                )
        node_executor = _NodeExecutor(node)
        self._executors[node] = node_executor
        try:
            return await node_executor.run_async(
                results, context, depth_from_last, **self._node_kwargs
            )
        except BaseException as e:
            self._stopped = True
            trace = traceback.format_exception(*sys.exc_info())
            logger.error(
                f"Stopping Graph, Error in node {node.label}. message: {e}\ntrace:\n{trace}"
            )
            return


class GraphExperiment(ExperimentDefinition):
    def __init__(
        self,
        topology: Optional[LabTopology],
        graph: Union[Node, Graph],
        label: Optional[str] = None,
        story: str = None,
    ) -> None:
        super().__init__(topology, label, story)
        if isinstance(graph, Graph):
            self._graph: Graph = graph
        else:
            self._graph: Graph = Graph({graph})
        self._to_node: Optional[Node] = None

    def get_execution_instructions(self) -> ExperimentExecutor:
        return _AsyncGraphExecutor(self._graph, self._to_node, **self._kwargs)

    def get_execution_serializer(self) -> ExecutionSerializer:
        return ExecutionSerializer([jsonpickle.dumps(self._graph.nodes)])

    def run_to_node(
        self,
        node: Node,
        db: Optional[DataWriter] = None,
        label: Optional[str] = None,
        **kwargs,
    ):
        logger.info(f"Running node {node.label} and dependencies")
        self._to_node = node
        old_label = self.label
        if label:
            self.label = label
        try:
            return self.run(db, **kwargs)
        finally:
            self.label = old_label

import asyncio
import enum
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime
from inspect import signature, iscoroutinefunction, getfullargspec
from itertools import count
from typing import Optional, Dict, Any, List, Set, Union, Callable, Coroutine, Iterable

import jsonpickle

from quaentropy.api.data_reader import (
    SingleExperimentDataReader,
    DataReader,
    ResultRecord,
)
from quaentropy.api.data_writer import (
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

    async def execute_async(
        self,
        parents_results: List[Dict[str, Any]],
        context: EntropyContext,
        node_execution_id: int,
        is_last,
        **kwargs,
    ) -> Dict[str, Any]:
        args_parameters, keyword_function_parameters = self._prepare_for_execution(
            context, is_last, kwargs, parents_results
        )

        try:
            if iscoroutinefunction(self._program):
                results = await self._program(
                    *args_parameters, **keyword_function_parameters
                )
            else:
                results = self._program(*args_parameters, **keyword_function_parameters)

            return self._handle_results(results)
        except BaseException as e:
            raise e

    def execute(
        self,
        parents_results: List[Dict[str, Any]],
        context: EntropyContext,
        node_execution_id: int,
        is_last,
        **kwargs,
    ) -> Dict[str, Any]:
        args_parameters, keyword_function_parameters = self._prepare_for_execution(
            context, is_last, kwargs, parents_results
        )

        try:
            if iscoroutinefunction(self._program):
                results = asyncio.run(
                    self._program(*args_parameters, **keyword_function_parameters)
                )
            else:
                results = self._program(*args_parameters, **keyword_function_parameters)

            return self._handle_results(results)
        except BaseException as e:
            raise e

    def _handle_results(self, results):
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

    def _prepare_for_execution(self, context, is_last, kwargs, parents_results):
        function_parameters = {k: v for d in parents_results for k, v in d.items()}
        sig = signature(self._program)
        for param in sig.parameters:
            if sig.parameters[param].annotation is EntropyContext:
                function_parameters[param] = context
        if "is_last" in sig.parameters:
            function_parameters["is_last"] = is_last
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
        return args_parameters, keyword_function_parameters


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

    async def execute_async(
        self,
        parents_results: List[Dict[str, Any]],
        context: EntropyContext,
        node_execution_id: int,
        is_last,
        **kwargs,
    ) -> Dict[str, Any]:
        return await _AsyncGraphExecutor(self._graph, **kwargs).execute_async(context)

    def execute(
        self,
        parents_results: List[Dict[str, Any]],
        context: EntropyContext,
        node_execution_id: int,
        is_last,
        **kwargs,
    ) -> Dict[str, Any]:
        return asyncio.run(
            _AsyncGraphExecutor(self._graph, **kwargs).execute_async(context)
        )


class _NodeExecutor:
    def __init__(self, node: Node, node_execution_id: int) -> None:
        super().__init__()
        self._node: Node = node
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
        self.result: Dict[str, Any] = {}
        self.to_run = True
        self._node_execution_id = node_execution_id

    def run(
        self,
        parents_results: List[Dict[str, Any]],
        context: EntropyContext,
        is_last: int,
        **kwargs,
    ) -> Dict[str, Any]:
        if self.to_run:
            self._prepare_for_run()
            self.result = self._node.execute(
                parents_results,
                context,
                self._node_execution_id,
                is_last,
                **kwargs,
            )
            return self._handle_result(context)

    async def run_async(
        self,
        parents_results: List[Dict[str, Any]],
        context: EntropyContext,
        is_last: int,
        **kwargs,
    ) -> Dict[str, Any]:
        if self.to_run:
            self._prepare_for_run()
            self.result = await self._node.execute_async(
                parents_results,
                context,
                self._node_execution_id,
                is_last,
                **kwargs,
            )
            return self._handle_result(context)

    def _handle_result(self, context):
        # logger fetching results
        for output_id in self.result:
            output = self.result[output_id]
            context.add_result(
                RawResultData(
                    label=f"output_{output_id}",
                    data=output,
                    stage=self._node_execution_id,
                )
            )
        self._end_time = datetime.now()
        logger.debug(
            f"Done running node <{self._node.__class__.__name__}> {self._node.label}"
        )
        return self.result

    def _prepare_for_run(self):
        logger.info(
            f"Running node <{self._node.__class__.__name__}> {self._node.label}"
        )
        self._start_time = datetime.now()
        logger.debug(
            f"Saving metadata before running node "
            f"<{self._node.__class__.__name__}> {self._node.label}"
        )


class _AsyncGraphExecutor(ExperimentExecutor):
    def __init__(self, graph: Graph, **kwargs) -> None:
        super().__init__()
        self._graph: Graph = graph
        self._node_kwargs = kwargs
        self._tasks: Dict[Node, Any] = dict()
        self._results: Dict = dict()
        self._stopped = False
        self._node_id_iter = count(start=0, step=1)
        self._executors: Dict[Node, _NodeExecutor] = dict()
        for node in self._graph.nodes:
            self._executors[node] = _NodeExecutor(node, next(self._node_id_iter))

    def execute(self, context: EntropyContext) -> Any:
        async_result = asyncio.run(self.execute_async(context))
        return async_result

    @property
    def failed(self) -> bool:
        return self._stopped

    async def execute_async(self, context: EntropyContext):
        # traverse the graph and run the nodes
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
        self, node: Node, context: EntropyContext, is_last: int
    ):
        tasks = []
        for parent in node.get_parents():
            if parent not in self._tasks:
                task = self._run_node_and_ancestors(parent, context, is_last + 1)
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
        node_executor = self._executors[node]
        try:
            return await node_executor.run_async(
                results, context, node in self._graph.end_nodes(), **self._node_kwargs
            )
        except BaseException as e:
            self._stopped = True
            trace = traceback.format_exception(*sys.exc_info())
            logger.error(
                f"Stopping Graph, Error in node {node.label}. message: {e}\ntrace:\n{trace}"
            )
            return


class _GraphExecutor(ExperimentExecutor):
    def __init__(self, graph: Graph, **kwargs) -> None:
        super().__init__()
        self._graph: Graph = graph
        self._node_kwargs = kwargs
        self._tasks: Dict[Node, Any] = dict()
        self._results: Dict = dict()
        self._stopped = False
        self._node_id_iter = count(start=0, step=1)
        self._executors: Dict[Node, _NodeExecutor] = dict()
        for node in self._graph.nodes:
            self._executors[node] = _NodeExecutor(node, next(self._node_id_iter))

    def execute(self, context: EntropyContext) -> Any:
        sorted_nodes = self._graph.nodes_in_topological_order()

        results = []
        for node in sorted_nodes:
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
            node_executor = self._executors[node]
            try:
                node_executor.run(
                    results,
                    context,
                    node in self._graph.end_nodes(),
                    **self._node_kwargs,
                )
            except BaseException as e:
                self._stopped = True
                trace = traceback.format_exception(*sys.exc_info())
                logger.error(
                    f"Stopping Graph, Error in node {node.label}. message: {e}\ntrace:\n{trace}"
                )
                return
        combined_result = {}
        for node in self._graph.end_nodes():
            result = self._executors[node].result
            if result:
                for output in self._graph.plot_outputs:
                    if output in result:
                        context.add_plot(
                            Plot(
                                label=f"graph result {output}",
                                data=result[output],
                                data_type=PlotDataType.np_2d,
                                bokeh_generator=BokehCirclePlotGenerator(),
                            )
                        )
                for key in result:
                    combined_result[key] = result[key]

        return combined_result

    @property
    def failed(self) -> bool:
        return self._stopped

    async def _run_node_and_ancestors(
        self, node: Node, context: EntropyContext, is_last: int
    ):
        tasks = []
        for parent in node.get_parents():
            if parent not in self._tasks:
                task = self._run_node_and_ancestors(parent, context, is_last + 1)
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
        node_executor = self._executors[node]
        try:
            return await node_executor.run_async(
                results, context, is_last, **self._node_kwargs
            )
        except BaseException as e:
            self._stopped = True
            trace = traceback.format_exception(*sys.exc_info())
            logger.error(
                f"Stopping Graph, Error in node {node.label}. message: {e}\ntrace:\n{trace}"
            )
            return


@dataclass
class NodeResults:
    node: Node
    execution_id: int
    results: Iterable[ResultRecord]


class SingleGraphExperimentDataReader(SingleExperimentDataReader):
    def __init__(
        self, experiment_id: int, db: DataReader, executor: _AsyncGraphExecutor
    ) -> None:
        super().__init__(experiment_id, db)
        self._executor = executor

    def get_results_from_node(
        self, node_label: str, result_label: Optional[str] = None
    ) -> Iterable[NodeResults]:
        # TODO should get from db and not from executor
        nodes = list(
            node for node in self._executor._graph.nodes if node.label == node_label
        )
        nodes_results = []
        for node in nodes:
            node_executor = self._executor._executors[node]
            execution_id = node_executor._node_execution_id
            nodes_results.append(
                NodeResults(
                    node,
                    execution_id,
                    self._data_reader.get_results(
                        self._experiment_id,
                        label=result_label,
                        stage=execution_id,
                    ),
                )
            )
        return nodes_results


class GraphSerializer:
    def __init__(self, graph: Graph, executor: _AsyncGraphExecutor, to_node) -> None:
        super().__init__()
        self._graph = graph
        self._executor = executor
        self._to_node = to_node

    def serialize(self) -> str:
        return f"""
dot:
{self._graph.export_dot_graph()}

nodes:
{[jsonpickle.dumps(node) for node in self._graph.nodes]}

nodes_Execution_ids:


        """


class GraphExecutionType(enum.Enum):
    Sync = 1
    Async = 2


class GraphExperiment(ExperimentDefinition):
    def __init__(
        self,
        topology: Optional[LabTopology],
        graph: Union[Node, Graph],
        label: Optional[str] = None,
        story: str = None,
        execution_type: GraphExecutionType = GraphExecutionType.Sync,
    ) -> None:
        super().__init__(topology, label, story)
        if isinstance(graph, Graph):
            self._graph: Graph = graph
        else:
            self._graph: Graph = Graph({graph})
        self._to_node: Optional[Node] = None
        self._execution_type: GraphExecutionType = execution_type

    def get_execution_instructions(self) -> ExperimentExecutor:
        if self._to_node:
            graph = self._to_node
        else:
            graph = self._graph
        if self._execution_type == GraphExecutionType.Sync:
            return _GraphExecutor(graph, **self._kwargs)
        elif self._execution_type == GraphExecutionType.Async:
            return _AsyncGraphExecutor(graph, **self._kwargs)
        else:
            raise Exception(f"Execution type {self._execution_type} is not supported")

    def serialize(self, executor) -> str:
        return GraphSerializer(self._graph, executor, self._to_node).serialize()

    def get_data_reader(self, exp_id, db, executor) -> SingleExperimentDataReader:
        return SingleGraphExperimentDataReader(exp_id, db, executor)

    def run_to_node(
        self,
        node: Node,
        db: Optional[DataWriter] = None,
        label: Optional[str] = None,
        **kwargs,
    ):
        if node not in self._graph.nodes:
            raise KeyError("Node is not in graph")
        logger.info(f"Running node {node.label} and dependencies")
        nodes = {node}
        self._fill_list_of_parents(nodes, node)

        self._to_node = Graph(nodes, self._graph.label, self._graph.plot_outputs)
        old_label = self.label
        if label:
            self.label = label
        try:
            return self.run(db, **kwargs)
        finally:
            self.label = old_label
            self._to_node = None

    def _fill_list_of_parents(self, list: Set, node):
        for parent in node.get_parents():
            if parent not in list:
                list.add(parent)
                self._fill_list_of_parents(list, parent)
import asyncio
import enum
import sys
import traceback
from datetime import datetime
from inspect import signature, iscoroutinefunction, getfullargspec
from itertools import count
from typing import Optional, Dict, Any, Set, Union, Callable, Coroutine, Iterable

from quaentropy.api.data_reader import (
    SingleExperimentDataReader,
    DataReader,
    NodeResults,
)
from quaentropy.api.data_writer import RawResultData, DataWriter, NodeData
from quaentropy.api.errors import EntropyError
from quaentropy.api.execution import ExperimentExecutor, EntropyContext
from quaentropy.api.experiment import ExperimentDefinition
from quaentropy.api.graph import Graph, Node, Output
from quaentropy.instruments.lab_topology import ExperimentResources
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
        input_values: Dict[str, Any],
        context: EntropyContext,
        node_execution_id: int,
        is_last,
        **kwargs,
    ) -> Dict[str, Any]:
        args_parameters, keyword_function_parameters = self._prepare_for_execution(
            context, is_last, kwargs, input_values
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
        input_values: Dict[str, Any],
        context: EntropyContext,
        node_execution_id: int,
        is_last,
        **kwargs,
    ) -> Dict[str, Any]:
        args_parameters, keyword_function_parameters = self._prepare_for_execution(
            context, is_last, kwargs, input_values
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

    def _prepare_for_execution(
        self, context, is_last, kwargs, input_values: Dict[str, Any]
    ):
        sig = signature(self._program)
        keyword_function_parameters = {}
        for param in sig.parameters:
            if sig.parameters[param].annotation is EntropyContext:
                keyword_function_parameters[param] = context
        if "is_last" in sig.parameters:
            keyword_function_parameters["is_last"] = is_last
        (
            args,
            varargs,
            varkw,
            defaults,
            kwonlyargs,
            kwonlydefaults,
            annotations,
        ) = getfullargspec(self._program)
        for arg in args + kwonlyargs:
            if (
                arg not in input_values
                and arg not in keyword_function_parameters
                and arg not in kwargs
                and (
                    (defaults and arg not in defaults)
                    or (kwonlydefaults and arg not in kwonlydefaults)
                )
            ):
                logger.error(f"Error in node {self.label} - {arg} is not in parameters")
                raise KeyError(arg)

            if arg in input_values:
                keyword_function_parameters[arg] = input_values[arg]
            elif arg in kwargs:
                keyword_function_parameters[arg] = kwargs[arg]
        args_parameters = []
        if varargs is not None:
            for item in input_values:
                if item not in keyword_function_parameters:
                    args_parameters.append(input_values[item])
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
        input_values: Dict[str, Any],
        context: EntropyContext,
        node_execution_id: int,
        is_last,
        **kwargs,
    ) -> Dict[str, Any]:
        return await _AsyncGraphExecutor(self._graph, **kwargs).execute_async(context)

    def execute(
        self,
        input_values: Dict[str, Any],
        context: EntropyContext,
        node_execution_id: int,
        is_last,
        **kwargs,
    ) -> Dict[str, Any]:
        return asyncio.run(
            _AsyncGraphExecutor(self._graph, **kwargs).execute_async(context)
        )


class _NodeExecutor:
    def __init__(self, node: Node, node_execution_id: int, is_key_node: bool) -> None:
        super().__init__()
        self._node: Node = node
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
        self.result: Dict[str, Any] = {}
        self.to_run = True
        self._node_execution_id = node_execution_id
        self._is_key_node = is_key_node

    def run(
        self,
        input_values: Dict[str, Any],
        context: EntropyContext,
        is_last: int,
        **kwargs,
    ) -> Dict[str, Any]:
        if self.to_run:
            self._prepare_for_run(context)
            self.result = self._node.execute(
                input_values,
                context,
                self._node_execution_id,
                is_last,
                **kwargs,
            )
            return self._handle_result(context)

    async def run_async(
        self,
        input_values: Dict[str, Any],
        context: EntropyContext,
        is_last: int,
        **kwargs,
    ) -> Dict[str, Any]:
        if self.to_run:
            self._prepare_for_run(context)
            self.result = await self._node.execute_async(
                input_values,
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
                    label=f"{output_id}",
                    data=output,
                    stage=self._node_execution_id,
                )
            )
        self._end_time = datetime.now()
        logger.debug(
            f"Done running node <{self._node.__class__.__name__}> {self._node.label}"
        )
        return self.result

    def _prepare_for_run(self, context):
        logger.info(
            f"Running node <{self._node.__class__.__name__}> {self._node.label}"
        )
        self._start_time = datetime.now()
        logger.debug(
            f"Saving metadata before running node "
            f"<{self._node.__class__.__name__}> {self._node.label} id={self._node_execution_id}"
        )
        context._data_writer.save_node(
            context._exp_id,
            NodeData(
                self._node_execution_id,
                self._start_time,
                self._node.label,
                self._is_key_node,
            ),
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
            self._executors[node] = _NodeExecutor(
                node, next(self._node_id_iter), node in self._graph.key_nodes
            )

    def execute(self, context: EntropyContext) -> Any:
        async_result = asyncio.run(self.execute_async(context))
        return async_result

    @property
    def failed(self) -> bool:
        return self._stopped

    async def execute_async(self, context: EntropyContext):
        # traverse the graph and run the nodes
        end_nodes = self._graph.end_nodes

        chains = []
        for node in end_nodes:
            chains.append(self._run_node_and_ancestors(node, context, 0))

        result = await asyncio.gather(*chains)
        result = [x for x in result if x is not None]
        combined_result = {}

        if result:
            combined_result = {k: v for d in result for k, v in d.items()}
        return combined_result

    async def _run_node_and_ancestors(
        self, node: Node, context: EntropyContext, is_last: int
    ):
        tasks = []
        for input_name in node.get_parents():
            if input_name not in self._tasks:
                task = self._run_node_and_ancestors(input_name, context, is_last + 1)
                tasks.append(task)
                self._tasks[input_name] = task
            else:
                tasks.append(self._tasks[input_name])
        results = []
        if len(tasks) > 0:
            await asyncio.wait(tasks)
            if self._stopped:
                return None
            results = {}
            inputs_by_name = node.get_inputs_by_name()
            for input_name in inputs_by_name:
                parent_node = inputs_by_name[input_name].node
                parent_output_name = inputs_by_name[input_name].name
                if (
                    parent_node not in self._executors
                    or parent_output_name not in self._executors[parent_node].result
                ):
                    raise EntropyError(
                        f"node {node.label} input is missing: {parent_output_name}"
                    )
                results[input_name] = self._executors[parent_node].result[
                    parent_output_name
                ]

        node_executor = self._executors[node]
        try:
            return await node_executor.run_async(
                results, context, node in self._graph.end_nodes, **self._node_kwargs
            )
        except BaseException as e:
            self._stopped = True
            trace = traceback.format_exception(*sys.exc_info())
            logger.error(
                f"Stopping Graph, Error in node {node.label} "
                f"of type {e.__class__.__qualname__}. message: {e}\ntrace:\n{trace}"
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
            self._executors[node] = _NodeExecutor(
                node, next(self._node_id_iter), node in self._graph.key_nodes
            )

    def execute(self, context: EntropyContext) -> Any:
        sorted_nodes = self._graph.nodes_in_topological_order()

        for node in sorted_nodes:
            results = {}
            inputs_by_name = node.get_inputs_by_name()
            for input_name in inputs_by_name:
                parent_node = inputs_by_name[input_name].node
                parent_output_name = inputs_by_name[input_name].name
                if (
                    parent_node not in self._executors
                    or parent_output_name not in self._executors[parent_node].result
                ):
                    raise EntropyError(
                        f"node {node.label} input is missing: {parent_output_name}"
                    )
                results[input_name] = self._executors[parent_node].result[
                    parent_output_name
                ]
            node_executor = self._executors[node]
            try:
                node_executor.run(
                    results,
                    context,
                    node in self._graph.end_nodes,
                    **self._node_kwargs,
                )
            except BaseException as e:
                self._stopped = True
                trace = traceback.format_exception(*sys.exc_info())
                logger.error(
                    f"Stopping Graph, Error in node {node.label} of type "
                    f"{e.__class__.__qualname__}. message: {e}\ntrace:\n{trace}"
                )
                return
        combined_result = {}
        for node in self._graph.end_nodes:
            result = self._executors[node].result
            if result:
                for key in result:
                    combined_result[key] = result[key]

        return combined_result

    @property
    def failed(self) -> bool:
        return self._stopped


class SingleGraphExperimentDataReader(SingleExperimentDataReader):
    def __init__(self, experiment_id: int, db: DataReader) -> None:
        super().__init__(experiment_id, db)

    def get_results_from_node(
        self, node_label: str, result_label: Optional[str] = None
    ) -> Iterable[NodeResults]:
        return self._data_reader.get_results_from_node(
            node_label, self._experiment_id, result_label
        )


class GraphExecutionType(enum.Enum):
    Sync = 1
    Async = 2


class GraphExperiment(ExperimentDefinition):
    def __init__(
        self,
        resources: Optional[ExperimentResources],
        graph: Union[Node, Graph],
        label: Optional[str] = None,
        story: str = None,
        execution_type: GraphExecutionType = GraphExecutionType.Sync,
    ) -> None:
        super().__init__(resources, label, story)
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
        if self._to_node:
            graph = self._to_node
        else:
            graph = self._graph
        return str(graph.export_dot_graph())

    def get_data_reader(self, exp_id, db, executor) -> SingleExperimentDataReader:
        return SingleGraphExperimentDataReader(exp_id, db)

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

        self._to_node = Graph(nodes, self._graph.label, self._graph.key_nodes)
        old_label = self.label
        if label:
            self.label = label
        try:
            return self.run(db, **kwargs)
        finally:
            self.label = old_label
            self._to_node = None

    def _fill_list_of_parents(self, parent_list: Set, node):
        for parent in node.get_parents():
            if parent not in parent_list:
                parent_list.add(parent)
                self._fill_list_of_parents(parent_list, parent)

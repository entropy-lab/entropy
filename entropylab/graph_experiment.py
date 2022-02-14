import asyncio
import enum
import sys
import time
import traceback
from copy import deepcopy
from datetime import datetime
from inspect import signature, iscoroutinefunction, getfullargspec
from itertools import count
from typing import Optional, Dict, Any, Set, Union, Callable, Coroutine, Iterable

from graphviz import Digraph

from entropylab.api.data_reader import (
    DataReader,
    NodeResults,
    ExperimentReader,
)
from entropylab.api.data_writer import DataWriter, NodeData
from entropylab.api.errors import EntropyError
from entropylab.api.execution import (
    ExperimentExecutor,
    EntropyContext,
    _EntropyContextFactory,
)
from entropylab.api.experiment import (
    ExperimentDefinition,
    ExperimentHandle,
    _Experiment,
)
from entropylab.api.graph import (
    GraphHelper,
    Node,
    Output,
    _NodeExecutionInfo,
    RetryBehavior,
)
from entropylab.instruments.lab_topology import ExperimentResources
from entropylab.logger import logger


def _handle_wait_time(wait_time, backoff, added_delay, maximum_wait_time):
    wait_time *= backoff
    wait_time += added_delay
    if maximum_wait_time is not None:
        wait_time = min(wait_time, maximum_wait_time)
    return wait_time


def _retry(
    node_label: str,
    function,
    number_of_attempts,
    wait_time,
    maximum_wait_time=None,
    backoff: float = 1,
    added_delay: float = 0,
):
    for attempt in range(number_of_attempts):
        try:
            return function()
        except BaseException as e:
            if attempt == number_of_attempts:
                raise e
            else:
                logger.warning(
                    f"node {node_label} has error, retrying #{attempt + 1}"
                    f" in {wait_time} seconds : {e}"
                )
                time.sleep(wait_time)
                wait_time = _handle_wait_time(
                    wait_time, backoff, added_delay, maximum_wait_time
                )


def pynode(
    label: str,
    input_vars: Dict[str, Output] = None,
    output_vars: Set[str] = None,
    must_run_after: Set[Node] = None,
):
    """
        decorator for running using the given python function as a PyNode
    :param label: node label
    :param input_vars: dictionary of node inputs, keys are input names.
                        the input values should be defined as the following:
                            >>>input_vars={"a": node.outputs["x"]}
    :param output_vars: a set of outputs name. The given python function should
                        return a dictionary, which it's keys are the same as output vars
    :param must_run_after: A set of nodes. If those nodes are in the same graph,
                            current node will run after they finish execution.
    :return: node instance
    """

    def decorate(fn):
        return PyNode(label, fn, input_vars, output_vars, must_run_after)

    return decorate


class PyNode(Node):
    """
    Node that gets a python function or coroutine and wraps it with an entropy graph node.
    Node is defined by a python function, input and outputs.
    Entropy will call the function, filling the function parameters:
        1. If function parameter has the same name as node input,
           Entropy will pass the input value
        2. If function parameter is of type EntropyContext, Entropy will pass current context
        3. If function parameter has the same name as given experiment run kwargs,
           Entropy will pass the kwarg value
        4. if function parameter is of type *args,
           Entropy will pass all inputs that were not specified by name
        5. If function parameter name is "is_last",
           Entropy will pass True if this is the last node in the graph

    """

    def __init__(
        self,
        label: str = None,
        program: Union[Callable, Coroutine] = None,
        input_vars: Dict[str, Output] = None,
        output_vars: Set[str] = None,
        must_run_after: Set[Node] = None,
        save_results: bool = True,
        retry_on_error: RetryBehavior = None,
    ):
        """
            Node that gets a python function or coroutine and wraps
            it with an entropy graph node.
            Node is defined by a python function, input and outputs.
            Entropy will call the function, filling the function parameters:
                1. If function parameter has the same name as node input,
                   Entropy will pass the input value
                2. If function parameter is of type EntropyContext,
                   Entropy will pass current context
                3. If function parameter has the same name as given
                   experiment run kwargs, Entropy will pass the kwarg value
                4. if function parameter is of type *args,
                   Entropy will pass all inputs that were not specified by name
                5. If function parameter name is "is_last",
                   Entropy will pass True if this is the last node in the graph
        :param label:  node label
        :param program: the node program in a python function or coroutine
        :param input_vars: dictionary of node inputs, keys are input names.
                        the input values should be defined as the following:
                            >>>input_vars={"a": node.outputs["x"]}
        :param output_vars: a set of outputs name. The given python function should
                        return a dictionary, which it's keys are the same as output vars
        :param must_run_after: A set of nodes. If those nodes are in the same graph,
                            current node will run after they finish execution.
        """
        super().__init__(
            label, input_vars, output_vars, must_run_after, save_results, retry_on_error
        )
        self._program = program

    async def _execute_async(
        self,
        input_values: Dict[str, Any],
        context: EntropyContext,
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

    def _execute(
        self,
        input_values: Dict[str, Any],
        context: EntropyContext,
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


def _create_actual_graph(nodes: Set[Node], key_nodes: Set[Node]):
    nodes_copy: Dict[Node, Node] = {node: deepcopy(node) for node in nodes}
    for node in nodes_copy:
        for input_var in nodes_copy[node]._input_vars:
            output = node._input_vars[input_var]
            nodes_copy[node]._input_vars[input_var] = nodes_copy[output.node].outputs[
                output.name
            ]
        nodes_copy[node]._must_run_after = {nodes_copy[m] for m in node._must_run_after}
    return {_NodeExecutionInfo(nodes_copy[node], node in key_nodes) for node in nodes}


class SubGraphNode(Node):
    """
    Node that holds a complete graph and runs as a single node within
    another graph.
    """

    def __init__(
        self,
        graph: Union[GraphHelper, Node, Set[Node]],
        label: str = None,
        input_vars: Dict[str, Output] = None,
        output_vars: Set[str] = None,
        must_run_after: Set[Node] = None,
        key_nodes: Optional[Set[Node]] = None,
        save_results: bool = True,
        retry_on_error: RetryBehavior = None,
    ):
        """

        :param graph: the graph model
        :param label: node label
        :param input_vars: dictionary of node inputs, keys are input names.
                        the input values should be defined as the following:
                            >>>input_vars={"a": node.outputs["x"]}
        :param output_vars: a set of outputs name. The given python function should
                        return a dictionary, which it's keys are the same as output vars
        :param must_run_after: A set of nodes. If those nodes are in the same graph,
                            current node will run after they finish execution.
        """
        super().__init__(
            label, input_vars, output_vars, must_run_after, save_results, retry_on_error
        )
        self._key_nodes = key_nodes
        if self._key_nodes is None:
            self._key_nodes = set()

        if isinstance(graph, GraphHelper):
            self._graph: Set[_NodeExecutionInfo] = _create_actual_graph(
                graph.nodes, self._key_nodes
            )
        elif isinstance(graph, Node):
            self._graph: Set[_NodeExecutionInfo] = _create_actual_graph(
                {graph}, self._key_nodes
            )
        elif isinstance(graph, Set):
            self._graph: Set[_NodeExecutionInfo] = _create_actual_graph(
                graph, self._key_nodes
            )
        else:
            raise Exception(
                "graph parameter type is not supported, please pass a Node or set of nodes"
            )

    async def _execute_async(
        self,
        input_values: Dict[str, Any],
        context: EntropyContext,
        is_last,
        **kwargs,
    ) -> Dict[str, Any]:
        executors = {node.node: _NodeExecutor(node) for node in self._graph}
        return await _AsyncGraphExecutor(
            self._graph, executors, **kwargs
        ).execute_async(context._context_factory)

    def _execute(
        self,
        input_values: Dict[str, Any],
        context: EntropyContext,
        is_last,
        **kwargs,
    ) -> Dict[str, Any]:
        executors = {node.node: _NodeExecutor(node) for node in self._graph}
        return _GraphExecutor(self._graph, executors, **kwargs).execute(
            context._context_factory
        )


class _NodeExecutor:
    def __init__(self, node_execution_info: _NodeExecutionInfo) -> None:
        super().__init__()
        self._node: Node = node_execution_info.node
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
        self.result: Dict[str, Any] = {}
        self.to_run = True
        self._is_key_node = node_execution_info.is_key_node

    def run(
        self,
        input_values: Dict[str, Any],
        context_factory: _EntropyContextFactory,
        is_last: int,
        **kwargs,
    ) -> Dict[str, Any]:
        if self.to_run:
            context = context_factory.create()
            self._prepare_for_run(context)
            retry_behavior = self._node._retry_on_error_function()
            if retry_behavior is not None:
                self.result = _retry(
                    self._node.label,
                    lambda: self._node._execute(
                        input_values,
                        context,
                        is_last,
                        **kwargs,
                    ),
                    number_of_attempts=retry_behavior.number_of_attempts,
                    wait_time=retry_behavior.wait_time,
                    backoff=retry_behavior.backoff,
                    added_delay=retry_behavior.added_delay,
                    maximum_wait_time=retry_behavior.max_wait_time,
                )
            else:
                self.result = self._node._execute(
                    input_values,
                    context,
                    is_last,
                    **kwargs,
                )
            return self._handle_result(context)

    async def run_async(
        self,
        input_values: Dict[str, Any],
        context_factory: _EntropyContextFactory,
        is_last: int,
        **kwargs,
    ) -> Dict[str, Any]:
        if self.to_run:
            context = context_factory.create()
            self._prepare_for_run(context)
            retry_behavior = self._node._retry_on_error_function()
            if retry_behavior is not None:
                self.result = await _retry(
                    self._node.label,
                    lambda: self._node._execute_async(
                        input_values,
                        context,
                        is_last,
                        **kwargs,
                    ),
                    number_of_attempts=retry_behavior.number_of_attempts,
                    wait_time=retry_behavior.wait_time,
                    backoff=retry_behavior.backoff,
                    added_delay=retry_behavior.added_delay,
                    maximum_wait_time=retry_behavior.max_wait_time,
                )
            else:
                self.result = await self._node._execute_async(
                    input_values,
                    context,
                    is_last,
                    **kwargs,
                )
            return self._handle_result(context)

    def _handle_result(self, context):
        if self._node._should_save_results():
            # logger fetching results
            for output_id in self.result:
                output = self.result[output_id]
                context.add_result(label=f"{output_id}", data=output)

        self._end_time = datetime.now()
        logger.debug(
            f"Done running node <{self._node.__class__.__name__}> {self._node.label}"
        )
        return self.result

    def _prepare_for_run(self, context: EntropyContext):
        logger.info(
            f"Running node <{self._node.__class__.__name__}> {self._node.label}"
        )
        self._start_time = datetime.now()
        logger.debug(
            f"Saving metadata before running node "
            f"<{self._node.__class__.__name__}> {self._node.label} id={context._get_stage_id()}"
        )
        context._data_writer.save_node(
            context._exp_id,
            NodeData(
                context._get_stage_id(),
                self._start_time,
                self._node.label,
                self._is_key_node,
            ),
        )


class _AsyncGraphExecutor(ExperimentExecutor):
    def __init__(
        self,
        nodes_execution_info: Set[_NodeExecutionInfo],
        nodes: Dict[Node, _NodeExecutor],
        **kwargs,
    ) -> None:
        super().__init__()
        self._graph: GraphHelper = GraphHelper(nodes_execution_info)
        self._node_kwargs = kwargs
        self._tasks: Dict[Node, Any] = dict()
        self._results: Dict = dict()
        self._stopped = False
        self._node_id_iter = count(start=0, step=1)
        self._executors: Dict[Node, _NodeExecutor] = nodes

    def execute(self, context: _EntropyContextFactory) -> Any:
        async_result = asyncio.run(self.execute_async(context))
        return async_result

    @property
    def failed(self) -> bool:
        return self._stopped

    async def execute_async(self, context_factory: _EntropyContextFactory):
        # traverse the graph and run the nodes
        end_nodes = self._graph.leaves

        chains = []
        for node in end_nodes:
            chains.append(self._run_node_and_ancestors(node, context_factory, 0))

        result = await asyncio.gather(*chains)
        result = [x for x in result if x is not None]
        combined_result = {}

        if result:
            combined_result = {k: v for d in result for k, v in d.items()}
        return combined_result

    async def _run_node_and_ancestors(
        self, node: Node, context_factory: _EntropyContextFactory, is_last: int
    ):
        tasks = []
        for input_name in node.get_parents():
            if input_name not in self._tasks:
                task = self._run_node_and_ancestors(
                    input_name, context_factory, is_last + 1
                )
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
                results,
                context_factory,
                node in self._graph.leaves,
                **self._node_kwargs,
            )
        except BaseException as e:
            self._stopped = True
            trace = "\n".join(traceback.format_exception(*sys.exc_info()))
            logger.error(
                f"Stopping GraphHelper, Error in node {node.label} "
                f"of type {e.__class__.__qualname__}. message: {e}\ntrace:\n{trace}"
            )
            return


class _GraphExecutor(ExperimentExecutor):
    def __init__(
        self,
        nodes_execution_info: Set[_NodeExecutionInfo],
        nodes: Dict[Node, _NodeExecutor],
        **kwargs,
    ) -> None:
        super().__init__()
        self._graph: GraphHelper = GraphHelper(nodes_execution_info)
        self._node_kwargs = kwargs
        self._tasks: Dict[Node, Any] = dict()
        self._results: Dict = dict()
        self._stopped = False
        self._executors: Dict[Node, _NodeExecutor] = nodes

    def execute(self, context_factory: _EntropyContextFactory) -> Any:
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
                    context_factory,
                    node in self._graph.leaves,
                    **self._node_kwargs,
                )
            except BaseException as e:
                self._stopped = True
                trace = "\n".join(traceback.format_exception(*sys.exc_info()))
                logger.error(
                    f"Stopping GraphHelper, Error in node {node.label} of type "
                    f"{e.__class__.__qualname__}. message: {e}\ntrace:\n{trace}"
                )
                return
        combined_result = {}
        for node in self._graph.leaves:
            result = self._executors[node].result
            if result:
                for key in result:
                    combined_result[key] = result[key]

        return combined_result

    @property
    def failed(self) -> bool:
        return self._stopped


class GraphReader(ExperimentReader):
    """
    Reads results and data from a single graph experiment
    """

    def __init__(self, experiment_id: int, db: DataReader) -> None:
        """
            Reads results and data from a single graph experiment
        :param experiment_id: the id of experiment
        :param db: results database that implemented the DataReader abstract class
        """
        super().__init__(experiment_id, db)

    def get_results_from_node(
        self, node_label: str, result_label: Optional[str] = None
    ) -> Iterable[NodeResults]:
        """
            returns an iterable of all results from a node with the given label
             and result with the given result_label
        :param node_label: label of node to get results from
        :param result_label: label of result records
        """
        return self._data_reader.get_results_from_node(
            node_label, self._experiment_id, result_label
        )


class GraphExperimentHandle(ExperimentHandle):
    """
    An handle of the graph experiment execution
    can be used to get information and read results
    """

    def __init__(self, experiment: _Experiment, graph: GraphHelper) -> None:
        super().__init__()
        self._experiment = experiment
        self._graph = graph

    @property
    def id(self):
        return self._experiment.exp_id

    @property
    def results(self) -> GraphReader:
        """
        returns a reader for reading results from a graph experiments
        """
        return GraphReader(self.id, self._experiment.data_reader())

    def dot_graph(self):
        return self._graph.export_dot_graph()


class GraphExecutionType(enum.Enum):
    Sync = 1
    Async = 2


class Graph(ExperimentDefinition):
    """
    Experiment defined by a graph model and runs within entropy.
    Information, results and metadata will be saved during every execution.
    Nodes within the graph will be executed in a topological order.
    Every node will be declared with a set of inputs and outputs, that
    will transfer for a node to it's children.
    """

    def __init__(
        self,
        resources: Optional[ExperimentResources],
        graph: Union[Node, Set[Node], GraphHelper],
        label: Optional[str] = None,
        story: str = None,
        key_nodes: Optional[Set[Node]] = None,
        execution_type: GraphExecutionType = GraphExecutionType.Sync,
        user: str = "",
    ) -> None:
        """
            Experiment defined by a graph model and runs within entropy.
        :param resources: shared lab resources or temporary resources that
                            are used in the experiment.
        :param graph: the experiment model, can be a graph or a single node.
        :param label: experiment label
        :param story: a description of the experiment, which will create an experiment story
                         with all other parts of the experiment
        :param key_nodes: a set of graph key nodes. those nodes will be marked as graph result.
        :param execution_type: specifty whether to run the graph in a sync mode, single node
                        on a given time, or asynchronously - which will run node in parallel
                        according to their dependency and implementation (using async.io)
        """
        super().__init__(resources, label, story, user)
        self._key_nodes = key_nodes
        if self._key_nodes is None:
            self._key_nodes = set()

        if isinstance(graph, GraphHelper):
            self._original_nodes: Set[Node] = graph.nodes
        elif isinstance(graph, Node):
            self._original_nodes: Set[Node] = {graph}
        elif isinstance(graph, Set):
            self._original_nodes: Set[Node] = graph
        else:
            raise Exception(
                "graph parameter type is not supported, please pass a Node or set of nodes"
            )

        all_ancestors = set(
            [anc for node in self._original_nodes for anc in node.ancestors()]
        )
        if all_ancestors != self._original_nodes:
            raise Exception(
                f"nodes has inputs that are not part of this graph: "
                f"{[node.label for node in all_ancestors.difference(self._original_nodes)]}"
            )

        self._actual_graph: Set[_NodeExecutionInfo] = _create_actual_graph(
            self._original_nodes, self._key_nodes
        )
        self._to_node: Optional[Node] = None
        self._execution_type: GraphExecutionType = execution_type

    def _get_execution_instructions(self) -> ExperimentExecutor:
        executors = {node.node: _NodeExecutor(node) for node in self._actual_graph}

        if self._execution_type == GraphExecutionType.Sync:
            return _GraphExecutor(self._actual_graph, executors, **self._kwargs)
        elif self._execution_type == GraphExecutionType.Async:
            return _AsyncGraphExecutor(self._actual_graph, executors, **self._kwargs)
        else:
            raise Exception(f"Execution type {self._execution_type} is not supported")

    def serialize(self) -> str:
        """
        dot graph representing the experiment
        """
        return str(GraphHelper(self._actual_graph).export_dot_graph())

    def run(self, db: Optional[DataWriter] = None, **kwargs) -> GraphExperimentHandle:
        experiment = self._run(db, **kwargs)
        return GraphExperimentHandle(experiment, GraphHelper(self._actual_graph))

    def run_to_node(
        self,
        node: Node,
        db: Optional[DataWriter] = None,
        label: Optional[str] = None,
        **kwargs,
    ) -> GraphExperimentHandle:
        """
            Run the experiment in Entropy environment, only with the given node and
            its ancestors.
            Every call to this function creates a new run and returns a different handle.

        :param node: the node object you want to run to.
        :param db: results db. if given, results will be saved in this DB. otherwise
                results will only be saved during this python session
        :param label: label for the current execution
        :param kwargs: key word arguments that will be passed to the experiment code as well.
                        user can specify here extra arguments, and request them in the
                        functions declarations.
        :return:a handle of the new graph experiment run
        """
        if node not in self._original_nodes:
            raise KeyError("Node is not in graph")
        logger.info(f"Running node {node.label} and dependencies")
        nodes = self._calculate_ancestors(node)
        full_graph = self._actual_graph
        self._actual_graph = _create_actual_graph(nodes, self._key_nodes)
        old_label = self.label
        if label:
            self.label = label
        try:
            return self.run(db, **kwargs)
        finally:
            self.label = old_label
            self._actual_graph = full_graph

    def _calculate_ancestors(self, node):
        ancestors: Set = set()
        for parent in node.ancestors():
            if parent in self._original_nodes:
                ancestors.add(parent)
        return ancestors

    def dot_graph(self) -> Digraph:
        return GraphHelper(self._actual_graph).export_dot_graph()

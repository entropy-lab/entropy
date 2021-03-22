import asyncio
import enum
from asyncio import Task
from datetime import datetime
from inspect import signature, iscoroutinefunction
from io import BytesIO
from typing import Optional, Dict, Any, List, Set, Union, Callable, Coroutine

from qm import _Program, QuantumMachine, SimulationConfig
from qm.QmJob import QmJob
from qm.pb.inc_qua_pb2 import QuaProgram
from quaentropy.api.data_writer import (
    ExecutionSerializer,
    RawResultData,
    PlotDataType,
    Plot,
    Metadata,
    DataWriter,
)
from quaentropy.api.errors import EntropyError
from quaentropy.api.execution import ExperimentExecutor, ExperimentRunningContext
from quaentropy.api.experiment import ExperimentDefinition
from quaentropy.api.graph import Graph, Node, Output
from quaentropy.api.plot import BokehCirclePlotGenerator
from quaentropy.instruments.lab_topology import LabTopology
from quaentropy.logger import logger


class AncestorRunStrategy(enum.Enum):
    RunAll = 1
    RunOnlyLast = 2


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
        context: ExperimentRunningContext,
        node_execution_id: int,
        depth,
        strategy,
    ) -> Dict[str, Any]:
        function_parameters = {k: v for d in parents_results for k, v in d.items()}

        sig = signature(self._program)

        # args, varargs, varkw, defaults, kwonlyargs, kwonlydefaults, annotations = inspect.getfullargspec(foo)
        if "context" in sig.parameters:
            function_parameters["context"] = context
        if "depth" in sig.parameters:
            function_parameters["depth"] = depth
        if "strategy" in sig.parameters:
            function_parameters["strategy"] = strategy

        if iscoroutinefunction(self._program):
            results = await self._program(**function_parameters)
        else:
            results = self._program(**function_parameters)
        outputs = {}
        for var in self._output_vars:
            try:
                outputs[var] = results[var]
            except KeyError:
                logger.error(
                    f"WARNING Could not fetch variable '{var}' from the results of node <{self.label}>"
                )
                pass
        return outputs


class QuaNode(Node):
    def __init__(
        self,
        label: str = None,
        qua_program: Union[_Program, Callable, Coroutine] = None,
        input_vars: List[Output] = None,
        output_vars: Set[str] = None,
        must_run_after: Set[Node] = None,
        quantum_machine: QuantumMachine = None,
        simulation_kwargs: SimulationConfig = None,
        execution_kwargs: Dict[str, Any] = None,
        simulate: bool = False,
    ):
        super().__init__(label, input_vars, output_vars, must_run_after)

        self._quantum_machine = quantum_machine
        self._execution_kwargs = execution_kwargs
        self._simulation_config = simulation_kwargs
        self._simulate: bool = simulate
        self._job: Optional[QmJob] = None
        self._qua_program: Optional[QuaProgram] = qua_program

    async def execute(
        self,
        parents_results: List[Dict[str, Any]],
        context: ExperimentRunningContext,
        node_execution_id: int,
        depth,
        strategy,
    ) -> Dict[str, Any]:
        # Get the Qua program that is wrapped by the python function
        function_parameters = {k: v for d in parents_results for k, v in d.items()}
        if iscoroutinefunction(self._qua_program):
            qua_program = await self._qua_program(function_parameters)
        elif isinstance(self._qua_program, Callable):
            qua_program = self._qua_program(function_parameters)
        else:
            qua_program = self._qua_program

        if not isinstance(qua_program, _Program):
            raise TypeError(
                f"Program given to node <{self._label}> must return a "
                f"Qua program as {_Program} but returns {type(qua_program)}"
            )

        if self._simulate and self._simulation_config is None:
            raise ValueError(
                f"Must specify simulation/execution parameters for QuaNode <{self._label}>"
            )

        if self._simulate:
            self._simulate_job(qua_program)
        else:
            self._execute_job(qua_program)

        outputs = {}
        for var in self._output_vars:
            try:
                self._job.result_handles.wait_for_all_values()
                output = self._job.result_handles.get(var).fetch_all(flat_struct=True)
                outputs[var] = output
            except AttributeError:
                logger.error(
                    f"WARNING Could not fetch variable '{var}' from the job results of node <{self.label}>"
                )

        # save all results as metadata
        res = self._job.result_handles
        npz_store = BytesIO()
        res.save_to_store(writer=npz_store)
        context.add_metadata(
            Metadata(
                label=f"node_{self._label} qua npz store",
                stage=node_execution_id,
                data=npz_store.getvalue(),
            )
        )
        return outputs

    def _execute_job(self, qua_program) -> None:
        logger.debug(f"Executing program in node {self.label}")
        self._job = self._quantum_machine.execute(
            self._qua_program, **self._execution_kwargs
        )

    def _simulate_job(self, qua_program) -> None:
        logger.debug(f"Simulating program in node {self.label}")
        self._job = self._quantum_machine.simulate(
            self._qua_program, self._simulation_config
        )


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
        context: ExperimentRunningContext,
        node_execution_id: int,
        depth,
        strategy,
    ) -> Dict[str, Any]:
        return await _AsyncGraphExecutor(self._graph).execute_async(context)


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
        context: ExperimentRunningContext,
        depth: int,
        strategy: AncestorRunStrategy,
    ) -> Dict[str, Any]:
        if self.to_run:
            logger.info(
                f"RUNNING {self._node.__class__.__name__} node <{self._node.label}>"
            )
            self._start_time = datetime.now()
            logger.debug(f"Saving metadata before running node <{self._node.label}>")
            node_execution_id = id(self)
            self.result = await self._node.execute(
                parents_results, context, node_execution_id, depth, strategy
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
            logger.info(
                f"Done RUNNING {self._node.__class__.__name__} <{self._node.label}>"
            )
            return self.result


class _AsyncGraphExecutor(ExperimentExecutor):
    def __init__(
        self,
        graph: Graph,
        to_node: Optional[Node] = None,
        ancestor_run_strategy: AncestorRunStrategy = AncestorRunStrategy.RunAll,
    ) -> None:
        super().__init__()
        self._graph: Graph = graph
        if to_node is not None and to_node not in self._graph.nodes:
            raise KeyError("Not is not in graph")

        self._to_node: Optional[Node] = to_node
        self._ancestor_run_strategy: AncestorRunStrategy = ancestor_run_strategy
        self._tasks: Dict[Node, Task] = dict()
        self._executors: Dict[Node, _NodeExecutor] = dict()
        self._results: Dict = dict()

    def execute(self, context: ExperimentRunningContext) -> Any:
        return asyncio.run(self.execute_async(context))

    async def execute_async(self, context: ExperimentRunningContext):
        # traverse the graph and run the nodes

        if self._graph is not Graph:
            # TODO execute node
            pass

        if self._to_node:
            end_nodes = self._to_node
        else:
            end_nodes = self._graph.end_nodes()

        chains = []
        for node in end_nodes:
            chains.append(self._run_node_and_ancestors(node, context, 0))

        def custom_exception_handler(loop, context):
            # first, handle with default handler
            loop.default_exception_handler(context)
            exception = context.get("exception")
            if isinstance(exception, EntropyError):
                logger.error(f"Error: {exception}")
                loop.stop()

        loop = asyncio.get_running_loop()

        # Set custom handler
        loop.set_exception_handler(custom_exception_handler)

        result = await asyncio.gather(*chains)
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
        self, node: Node, context: ExperimentRunningContext, depth: int
    ):
        tasks = []
        for parent in node.get_parents():
            if parent not in self._tasks:
                task = self._run_node_and_ancestors(parent, context, depth + 1)
                tasks.append(task)
                self._tasks[parent] = task
            else:
                tasks.append(self._tasks[parent])
        results = []
        if len(tasks) > 0:
            await asyncio.wait(tasks)
            results = []
            for parent in node.get_inputs():
                results.append(
                    {parent.name: self._executors[parent.node].result[parent.name]}
                )
        executor = _NodeExecutor(node)
        self._executors[node] = executor
        return await executor.run_async(
            results, context, depth, self._ancestor_run_strategy
        )


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
        self._ancestor_run_strategy: Optional[
            AncestorRunStrategy
        ] = AncestorRunStrategy.RunAll

    def get_execution_instructions(self) -> ExperimentExecutor:
        return _AsyncGraphExecutor(
            self._graph, self._to_node, self._ancestor_run_strategy
        )

    def get_execution_serializer(self) -> ExecutionSerializer:
        return ExecutionSerializer([self._graph])

    def run_to_node(
        self,
        node: Node,
        db: Optional[DataWriter] = None,
        ancestor_run_strategy: AncestorRunStrategy = AncestorRunStrategy.RunAll,
        label: Optional[str] = None,
    ):
        self._to_node = node
        self._ancestor_run_strategy = ancestor_run_strategy
        old_label = self.label
        self.label = label
        try:
            return self.run(db)
        finally:
            self.label = old_label

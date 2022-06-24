from entropylab.pipeline import Script, Graph
from entropylab.pipeline.api.data_reader import ExperimentReader
from entropylab.pipeline.api.execution import EntropyContext
from entropylab.pipeline.graph_experiment import PyNode
from entropylab.components.lab_topology import ExperimentResources, LabResources
from entropylab.pipeline.results_backend.sqlalchemy.db import SqlAlchemyDB


def a(context: EntropyContext):
    x = 1
    y = 2
    print(context.get_resource("res1"))
    print(context.get_resource("res2"))
    print(x + y)
    return {"a_out": 10}


def b(a):
    print(a)


def test_script():
    db = SqlAlchemyDB()

    lab = LabResources(db)
    lab.register_resource("res1", int, [5])

    experiment_resources = ExperimentResources(db)
    experiment_resources.import_lab_resource("res1")
    experiment_resources.add_temp_resource("res2", 42)

    model = Script(experiment_resources, a, "running the exp")
    script_experiment_handle = model.run(db)
    exp_id = script_experiment_handle.id
    results = script_experiment_handle.results

    reader = ExperimentReader(exp_id, db)


def test_graph():
    db = SqlAlchemyDB()

    lab = LabResources(db)
    lab.register_resource("res1", int, [5])

    experiment_resources = ExperimentResources(db)
    experiment_resources.import_lab_resource("res1")
    experiment_resources.add_temp_resource("res2", 42)

    node1 = PyNode("the first node", a, output_vars={"a_out"})
    node2 = PyNode("the second node", b, input_vars={"a": node1.outputs["a_out"]})

    model = Graph(
        experiment_resources, node2.ancestors(), "running the graph", key_nodes=set()
    )
    graph_handle = model.run(db)

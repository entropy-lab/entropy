import os

from quaentropy.api.graph import Graph
from quaentropy.graph_experiment import (
    GraphExperiment,
    PyNode,
    SingleGraphExperimentDataReader,
)
from quaentropy.results_backend.sqlalchemy.db import SqlAlchemyDB


def a():
    return {"x": 1}


def b():
    return {"y": 2}


def c():
    return {"z": 1.5}


def test_async_graph_single_data_reader():
    try:
        db = SqlAlchemyDB("test_running_db_graph.db")
        a1 = PyNode("a", a, output_vars={"x"})

        b1 = PyNode("b", b, output_vars={"y"})
        c1 = PyNode("c", c, output_vars={"z"})

        g = Graph({a1, b1, c1}, "hello", plot_outputs={"y_z"})

        run = GraphExperiment(None, g, "run_a").run(db)
        run = GraphExperiment(None, g, "run_a").run(db)
        reader: SingleGraphExperimentDataReader = run.results_reader()
        nodes_results = reader.get_results_from_node(node_label="c")
        for node_results in nodes_results:
            print(node_results.results)
            results = list(node_results.results)
            assert len(results) == 1
            assert results[0].label == "output_z"
            assert results[0].data == 1.5

        # GraphReader(db, exp_id).get_results_from_node(label="")
    finally:
        os.remove("test_running_db_graph.db")
        pass


def test_async_graph_multi_data_reader():
    try:
        db = SqlAlchemyDB("test_running_db_graph.db")
        a1 = PyNode("a", a, output_vars={"x"})

        b1 = PyNode("b", b, output_vars={"y"})
        c1 = PyNode("c", c, output_vars={"z"})

        g = Graph({a1, b1, c1}, "hello", plot_outputs={"y_z"})

        run = GraphExperiment(None, g, "run_a").run(db)
        run = GraphExperiment(None, g, "run_a").run(db)
        nodes_results = db.get_results_from_node(run._id, node_label="c")
        for node_results in nodes_results:
            print(node_results.results)
            results = list(node_results.results)
            assert len(results) == 1
            assert results[0].label == "output_z"
            assert results[0].data == 1.5
    finally:
        os.remove("test_running_db_graph.db")
        pass

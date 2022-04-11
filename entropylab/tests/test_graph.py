import asyncio
import os
from time import sleep

import pytest
from bokeh.io import save
from bokeh.plotting import Figure

from entropylab.api.data_writer import PlotSpec
from entropylab.api.execution import EntropyContext
from entropylab.api.plot import CirclePlotGenerator
from entropylab.graph_experiment import (
    Graph,
    PyNode,
    pynode,
    SubGraphNode,
)


async def a():
    rest = 1
    print(f"Node a resting for {rest}")
    await asyncio.sleep(rest)
    print(f"Node a finished resting")
    return {"x": rest}


async def b():
    rest = 2
    # m = m.result_handles
    print(f"Node b resting for {rest}")
    await asyncio.sleep(rest)
    print(f"Node b finished resting")
    return {"y": rest}


def c():
    rest = 0.2
    print(f"Node c resting for {rest}")
    sleep(rest)
    print(f"Node c finished resting")
    return {"z": rest}


def d(x, y):
    print(f"Node d resting for {x / y}")
    sleep(x / y)
    print(f"d Result: {x + y}")
    return {"x_y": x + y}


async def e(y, z, context: EntropyContext):
    print(f"Node e resting for {y / z}")
    print(f"e Result: {y + z}")
    context.add_plot(
        PlotSpec(CirclePlotGenerator, "the best plot"),
        data=[[0, 1, 2, 3, 4, 5], [y + z, 7, 6, 20, 10, 11]],
    )
    return {"y_z": [0, 1, 2, 3, 4, 5, y + z, 7, 6, 20, 10, 11]}


def process_y_z(y_z):
    return {"data": [y_z, y_z]}


def f(x):
    print(x)
    return {"y_z": x}


def f1(y_z):
    print(y_z)
    return {"y_z": y_z}


def test_sync_graph_short():
    a1 = PyNode("a", a, output_vars={"x"})
    f1 = PyNode("b", f, {"x": a1.outputs["x"]}, {"y_z"})
    handle = Graph(None, {a1, f1}, "run_a").run()
    reader = handle.results
    dot = handle.dot_graph()
    print(dot)

    print(reader.get_experiment_info())


@pynode("a", output_vars={"x"})
async def decor():
    rest = 1
    print(f"Node a resting for {rest}")
    await asyncio.sleep(rest)
    print(f"Node a finished resting")
    return {"x": rest}


@pynode("b", input_vars={"x": decor.outputs["x"]}, output_vars={"x"})
async def decor1(x):
    rest = 1
    print(f"Node b resting for {rest}")
    await asyncio.sleep(rest)
    print(f"Node b finished resting")
    return {"x": rest}


def test_sync_graph_short_decor():
    handle = Graph(None, {decor, decor1}, "run_a").run()
    results = handle.results
    dot = handle.dot_graph()
    print(dot)
    print(results.get_experiment_info())


def test_sync_graph_must_run_after():
    a1 = PyNode("a", a, output_vars={"x"})
    a2 = PyNode("a", a, output_vars={"x"}, must_run_after={a1})
    handle = Graph(None, {a1, a2}, "must_run_after").run()
    results = handle.results
    print(results.get_experiment_info())
    print(handle.dot_graph())


def test_sync_graph():
    a1 = PyNode("a", a, output_vars={"x"})

    b1 = PyNode("b", b, output_vars={"y"})
    # c1 = c('c', x=a1.outputs.x, y = b1.outputs.y)
    c1 = PyNode("c", c, output_vars={"z"})
    d1 = PyNode(
        "d",
        d,
        input_vars={"x": a1.outputs["x"], "y": b1.outputs["y"]},
        output_vars={"x_y"},
    )
    d2 = PyNode("d2", d, {"x": a1.outputs["x"], "y": b1.outputs["y"]}, {"x_y"})
    e1 = PyNode("e", e, {"y": b1.outputs["y"], "z": c1.outputs["z"]}, {"y_z"})
    e1 = PyNode("a", e, {"y": b1.outputs["y"], "z": c1.outputs["z"]}, {"y_z"})

    g = {a1, b1, c1, d1, d2, e1}
    graph = Graph(None, g, "run_a")
    dot = graph.dot_graph()
    print(dot)
    # dot.format ='png'
    # dot.view()

    results = graph.run().results
    print(results.get_experiment_info())

    results = Graph(None, g, "run_a").run().results
    print(results.get_experiment_info())
    # TODO: Use figures instead of plots here
    plots = results.get_plots()
    for plot in plots:
        figure = Figure()
        plot.generator.plot_bokeh(figure, plot.plot_data)
        if not os.path.exists("tests_cache"):
            os.mkdir("tests_cache")
        save(figure, f"tests_cache/bokeh-exported-{plot.label}.html")


def test_sync_graph_run_to_node():
    a1 = PyNode("a", a, output_vars={"x"})

    b1 = PyNode("b", b, output_vars={"y"})
    # c1 = c('c', x=a1.outputs.x, y = b1.outputs.y)
    c1 = PyNode("c", c, output_vars={"z"})
    d1 = PyNode(
        "d",
        d,
        input_vars={"x": a1.outputs["x"], "y": b1.outputs["y"]},
        output_vars={"x_y"},
    )
    d2 = PyNode("d2", d, {"x": a1.outputs["x"], "y": b1.outputs["y"]}, {"x_y"})
    e1 = PyNode("e", e, {"y": b1.outputs["y"], "z": c1.outputs["z"]}, {"y_z"})

    definition = Graph(None, {a1, b1, c1, d1, d2, e1}, "run_a")
    reader = definition.run_to_node(a1).results
    print(reader.get_experiment_info())
    reader = definition.run_to_node(b1, label="only b1").results
    print(reader.get_experiment_info())
    reader = definition.run_to_node(d1).results
    print(reader.get_experiment_info())


def test_sub_graph_node():
    b1 = PyNode("b", b, output_vars={"y"})
    c1 = PyNode("c", c, output_vars={"z"})
    e1 = PyNode("e", e, {"y": b1.outputs["y"], "z": c1.outputs["z"]}, {"y_z"})
    sub_g = SubGraphNode(e1.ancestors(), "sub_node", output_vars={"y_z"})
    f = PyNode("f", f1, {"y_z": sub_g.outputs["y_z"]})
    Graph(None, f.ancestors(), "whole").run()


def test_sync_graph_short_edit():
    a1 = PyNode("a", a, output_vars={"x"})
    f1 = PyNode("b", f, {"x": a1.outputs["x"]}, {"y_z"})
    graph = Graph(None, {a1, f1}, "run_a")
    c = PyNode("c", f, {"x": a1.outputs["x"]}, {"y_z"})
    f1._input_vars["x"] = c.outputs["y_z"]
    handle = graph.run()
    reader = handle.results
    dot = handle.dot_graph()
    print(dot)
    print(reader.get_experiment_info())

    # run again, now the graph will fail because the node had changed
    with pytest.raises(Exception):
        graph = Graph(None, {a1, f1}, "run_a")
        handle = graph.run()
        reader = handle.results
        dot = handle.dot_graph()
        print(dot)
        print(reader.get_experiment_info())

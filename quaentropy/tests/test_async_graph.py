import asyncio

import numpy as np
from bokeh.io import save
from bokeh.plotting import Figure

from quaentropy.api.graph import Graph
from quaentropy.graph_experiment import GraphExperiment, PyNode, pynode


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


async def c():
    rest = 1.5
    print(f"Node c resting for {rest}")
    await asyncio.sleep(rest)
    print(f"Node c finished resting")
    return {"z": rest}


async def d(x, y):
    print(f"Node d resting for {x / y}")
    await asyncio.sleep(x / y)
    print(f"d Result: {x + y}")
    return {"x_y": x + y}


async def e(y, z):
    print(f"Node e resting for {y / z}")
    await asyncio.sleep(y / z)
    print(f"e Result: {y + z}")
    return {"y_z": np.array([[0, 1, 2, 3, 4, 5], [y + z, 7, 6, 20, 10, 11]])}


def f(x):
    print(x)
    return {"y_z": x}  # guy how not to do that


def test_async_graph_short():
    a1 = PyNode("a", a, output_vars={"x"})

    f1 = PyNode("b", f, {"x": a1.outputs["x"]}, {"y_z"})

    g = Graph({a1, f1}, "hello")

    dot = g.export_dot_graph()
    print(dot)

    run = GraphExperiment(None, g, "run_a").run()
    print(run.results_reader().get_experiment_data())


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


def test_async_graph_short_decor():
    a1 = decor
    b1 = decor1

    g = Graph({a1, b1}, "hello")

    dot = g.export_dot_graph()
    print(dot)

    run = GraphExperiment(None, g, "run_a").run()
    print(run.results_reader().get_experiment_data())


def test_async_graph_must_run_after():
    a1 = PyNode("a", a, output_vars={"x"})
    a2 = PyNode("a", a, output_vars={"x"}, must_run_after={a1})
    g = Graph({a1, a2}, "must_run_after")

    run = GraphExperiment(None, g, "run_a").run()
    print(run.results_reader().get_experiment_data())
    print(g.export_dot_graph())


def test_async_graph():
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

    g = Graph({a1, b1, c1, d1, d2, e1}, "hello", plot_outputs={"y_z"})
    print()
    dot = g.export_dot_graph()
    print(dot)
    # dot.format ='png'
    # dot.view()

    run = GraphExperiment(None, g, "run_a").run()
    print(run.results_reader().get_experiment_data())
    plots = run.results_reader().get_plots()
    for plot in plots:
        figure = Figure()
        plot.bokeh_generator.plot_in_figure(figure, plot.plot_data, plot.data_type)
        save(figure, f"try{plot.label}.html")


def test_async_graph_run_to_node():
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

    g = Graph({a1, b1, c1, d1, d2, e1}, "hello", plot_outputs={"y_z"})
    definition = GraphExperiment(None, g, "run_a")
    reader = definition.run_to_node(a1).current_experiment_results()
    print(reader.get_experiment_data())
    reader = definition.run_to_node(b1, label="only b1").current_experiment_results()
    print(reader.get_experiment_data())
    reader = definition.run_to_node(d1).current_experiment_results()
    print(reader.get_experiment_data())

import asyncio
import os
from time import sleep

from bokeh.io import save
from bokeh.plotting import Figure

from quaentropy.api.data_writer import PlotSpec
from quaentropy.api.execution import EntropyContext
from quaentropy.api.graph import Graph
from quaentropy.api.plot import CirclePlotGenerator
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


def test_async_graph_short():
    a1 = PyNode("a", a, output_vars={"x"})

    f1 = PyNode("b", f, {"x": a1.outputs["x"]}, {"y_z"})

    g = Graph({a1, f1}, "hello")

    dot = g.export_dot_graph()
    print(dot)

    run = GraphExperiment(None, g, "run_a").run()
    print(run.results_reader().get_experiment_info())


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
    print(run.results_reader().get_experiment_info())


def test_sync_graph_must_run_after():
    a1 = PyNode("a", a, output_vars={"x"})
    a2 = PyNode("a", a, output_vars={"x"}, must_run_after={a1})
    g = Graph({a1, a2}, "must_run_after")

    run = GraphExperiment(None, g, "run_a").run()
    print(run.results_reader().get_experiment_info())
    print(g.export_dot_graph())


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

    g = Graph({a1, b1, c1, d1, d2, e1}, "hello")
    print()
    dot = g.export_dot_graph()
    print(dot)
    # dot.format ='png'
    # dot.view()

    run = GraphExperiment(None, g, "run_a").run()
    print(run.results_reader().get_experiment_info())

    run = GraphExperiment(None, g, "run_a").run()
    print(run.results_reader().get_experiment_info())
    plots = run.results_reader().get_plots()
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

    g = Graph({a1, b1, c1, d1, d2, e1}, "hello")
    definition = GraphExperiment(None, g, "run_a")
    reader = definition.run_to_node(a1).results_reader()
    print(reader.get_experiment_info())
    reader = definition.run_to_node(b1, label="only b1").results_reader()
    print(reader.get_experiment_info())
    reader = definition.run_to_node(d1).results_reader()
    print(reader.get_experiment_info())

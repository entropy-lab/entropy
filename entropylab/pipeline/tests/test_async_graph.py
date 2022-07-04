import asyncio

import numpy as np

from entropylab.pipeline.graph_experiment import (
    Graph,
    PyNode,
    pynode,
    GraphExecutionType,
    SubGraphNode,
)


async def a():
    rest = 0.001
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
    return {"y_z": x}


def f1(y_z):
    print(y_z)
    return {"y_z": y_z}


def test_async_graph_short():
    a1 = PyNode("a", a, output_vars={"x"})
    f1 = PyNode("b", f, {"x": a1.outputs["x"]}, {"y_z"})
    run = (
        Graph(None, {a1, f1}, "run_a", execution_type=GraphExecutionType.Async)
        .run()
        .results
    )
    print(run.get_experiment_info())


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
    handle = Graph(
        None, {decor, decor1}, "run_a", execution_type=GraphExecutionType.Async
    ).run()
    dot = handle.dot_graph()
    print(dot)
    results = handle.results
    print(results.get_experiment_info())


def test_async_graph_must_run_after():
    a1 = PyNode("a", a, output_vars={"x"})
    a2 = PyNode("a", a, output_vars={"x"}, must_run_after={a1})
    handle = Graph(
        None, {a1, a2}, "must_run_after", execution_type=GraphExecutionType.Async
    ).run()
    results = handle.results
    print(results.get_experiment_info())
    print(handle.dot_graph())


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

    graph = Graph(
        None, {a1, b1, c1, d1, d2, e1}, "run_a", execution_type=GraphExecutionType.Async
    )
    dot = graph.dot_graph()
    print(dot)
    # dot.format ='png'
    # dot.view()

    results = graph.run().results
    print(results.get_experiment_info())


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

    definition = Graph(
        None, {a1, b1, c1, d1, d2, e1}, "run_a", execution_type=GraphExecutionType.Async
    )
    reader = definition.run_to_node(a1).results
    print(reader.get_experiment_info())
    reader = definition.run_to_node(b1, label="only b1").results
    print(reader.get_experiment_info())
    reader = definition.run_to_node(d1).results
    print(reader.get_experiment_info())


def test_sub_graph_node_async():
    b1 = PyNode("b", b, output_vars={"y"})
    c1 = PyNode("c", c, output_vars={"z"})
    e1 = PyNode("e", e, {"y": b1.outputs["y"], "z": c1.outputs["z"]}, {"y_z"})
    sub_g = SubGraphNode(e1.ancestors(), "sub_node", output_vars={"y_z"})

    f = PyNode("f", f1, {"y_z": sub_g.outputs["y_z"]})

    Graph(None, f.ancestors(), "run_a", execution_type=GraphExecutionType.Async).run()

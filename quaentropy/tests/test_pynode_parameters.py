import asyncio

from quaentropy.api.graph import Graph
from quaentropy.graph_experiment import pynode, GraphExperiment


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


@pynode(
    "c",
    input_vars={"x": decor.outputs["x"], "y": decor1.outputs["x"]},
    output_vars={"x"},
)
async def decor2(*args):
    rest = 1
    print(f"Node b resting for {rest}")
    await asyncio.sleep(rest)
    print(f"Node b finished resting")
    return {"x": rest}


def test_async_graph_short_decor():
    g = Graph({decor, decor1, decor2}, "hello")
    GraphExperiment(None, g, "run_a").run()

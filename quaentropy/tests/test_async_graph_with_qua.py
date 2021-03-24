import asyncio
import os

from qm import LoopbackInterface
from qm import SimulationConfig
from qm.QuantumMachinesManager import QuantumMachinesManager
from qm.qua import *

from quaentropy.api.graph import Graph
from quaentropy.graph_experiment import GraphExperiment, PyNode, QuaNode
from quaentropy.results_backend.sqlalchemy.connector_and_topology import (
    SqlalchemySqlitePandasAndTopologyConnector,
)

config = {
    "version": 1,
    "controllers": {
        "con1": {
            "type": "opx1",
            "analog_outputs": {
                1: {"offset": +0.0},
                2: {"offset": +0.0},
                3: {"offset": +0.0},
            },
            "digital_outputs": {
                1: {},
            },
            "analog_inputs": {
                1: {"offset": +0.0},
            },
        }
    },
    "elements": {
        "qe1": {
            "singleInput": {"port": ("con1", 3)},
            "intermediate_frequency": 100e6,
            "operations": {
                "playOp": "constPulse",
            },
        },
        "qe2": {
            "singleInput": {"port": ("con1", 2)},
            "outputs": {"output1": ("con1", 1)},
            "intermediate_frequency": 100e6,
            "operations": {
                "readoutOp": "readoutPulse",
            },
            "time_of_flight": 28,
            "smearing": 0,
        },
    },
    "pulses": {
        "constPulse": {
            "operation": "control",
            "length": 1000,
            "waveforms": {"single": "const_wf"},
        },
        "readoutPulse": {
            "operation": "measure",
            "length": 1000,
            "waveforms": {"single": "const_wf"},
            "digital_marker": "ON",
            "integration_weights": {"x": "xWeights", "y": "yWeights"},
        },
    },
    "waveforms": {
        "const_wf": {"type": "constant", "sample": 0.4},
    },
    "digital_waveforms": {
        "ON": {"samples": [(1, 0)]},
    },
    "integration_weights": {
        "xWeights": {"cosine": [1.0] * 500, "sine": [0.0] * 500},
        "yWeights": {"cosine": [0.0] * 500, "sine": [1.0] * 500},
    },
}


async def a():
    rest = 5
    print(f"Node a resting for {rest}")
    await asyncio.sleep(rest)
    with program() as qua_prog:
        x = declare(fixed, value=rest)
        save(x, "x")
    return qua_prog


async def b():
    rest = 3
    # m = m.result_handles
    print(f"Node b resting for {rest}")
    await asyncio.sleep(rest)
    # time.sleep(rest)
    return {"y": rest}


async def c():
    rest = 1
    print(f"Node c resting for {rest}")
    await asyncio.sleep(rest)
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
    return {"y_z": y + z}


def f(x):
    print(x)
    return {"y_z": x}


def test_async_graph():
    try:
        # Open communication with the server.
        qmm = QuantumMachinesManager()

        # Create a quantum machine based on the configuration.

        QM = qmm.open_qm(config)

        sim_args = SimulationConfig(
            duration=int(1e5),
            simulation_interface=LoopbackInterface([("con1", 3, "con1", 1)]),
        )

        a1 = QuaNode(
            "a",
            a,
            None,
            {"x"},
            quantum_machine=QM,
            simulation_kwargs=sim_args,
            simulate=True,
        )
        b1 = PyNode("b", b, None, {"y"})
        c1 = PyNode("c", c, None, {"z"})
        d1 = PyNode("d", d, {"x": a1.outputs["x"], "y": b1.outputs["y"]}, {"x_y"})
        e1 = PyNode("e", d, {"y": b1.outputs["y"], "z": c1.outputs["z"]}, {"y_z"})
        g = Graph(label="hello", nodes={a1, c1, b1, d1, e1})

        db = SqlalchemySqlitePandasAndTopologyConnector("here1.db")

        GraphExperiment(None, g, "graph with qua").run(db)
        print(g.export_dot_graph())
    finally:
        os.remove("db_and_topo.db")
        pass

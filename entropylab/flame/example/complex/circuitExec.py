import entropylab.flame.nodeio as nodeio
import time
import random

# ==================== DEFINE NODE ====================

nodeio.context(
    name="QPUcircuitRunner",
    description="Executes given circuit sequence",
    icon="bootstrap/cpu.svg",
)

input = nodeio.Inputs()
input.stream("circuit_param", units="JSON", description="cirucuit description")
input.stream("error_correction", units="JSON", description="corrected data")

output = nodeio.Outputs()
output.define(
    "precorrected_data",
    units="JSON",
    description="data send for error correction",
    retention=2,
)
output.define(
    "circuit_finished", units="bool", description="Event trigger", retention=0
)
output.define(
    "final_data", units="bool", description="circuit averaged output", retention=2
)

nodeio.register()  # enables this to be used as part of workflow

# ==================== DRY-RUN DATA ====================

input.set(circuit_param=7.3)
input.set(
    error_correction=[[2, 1], [1, 2]],
)

# =============== RUN NODE STATE MACHINE ===============

while nodeio.status.active:
    circuit = input.get("circuit_param")
    print(f"running circuit param : {circuit}")
    time.sleep(0.1)

    output.set(
        precorrected_data=[
            [circuit + random.random() * 10, circuit / 2 + random.random() * 10],
            [circuit / 2 + random.random() * 10, circuit + random.random() * 10],
        ]
    )
    errorCorrected = input.get("error_correction")
    output.set(final_data=errorCorrected[0][0])
    output.set(circuit_finished=True)

# ================= =================== =================

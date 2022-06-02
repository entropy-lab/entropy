import entropylab.flame.nodeio as nodeio
import time
from math import floor

# ==================== DEFINE NODE ====================

nodeio.context(
    name="BayesianEstimation",
    description="error correction algorithm",
    icon="bootstrap/speedometer.svg",
)

input = nodeio.Inputs()
input.stream("precorrected_data", units="JSON", description="cirucuit description")

output = nodeio.Outputs()
output.define(
    "correction_data",
    units="JSON",
    description="corrected data",
    retention=2,
)

nodeio.register()  # enables this to be used as part of workflow

# ==================== DRY-RUN DATA ====================

input.set(
    precorrected_data=[
        [126.05530079997635, 125.08083240358124],
        [128.8241407925009, 123.69210583423587],
    ]
)

# =============== RUN NODE STATE MACHINE ===============

v = 0

while nodeio.status.active:
    data = input.get("precorrected_data")
    print(data)
    for i in [0, 1]:
        for j in [0, 1]:
            data[i][j] = floor(data[i][j])
    print(data)

    time.sleep(0.8)
    output.set(correction_data=data)
    v += 1

# ================= =================== =================

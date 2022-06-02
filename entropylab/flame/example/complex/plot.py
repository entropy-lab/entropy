import entropylab.flame.nodeio as nodeio

import numpy as np
import matplotlib.pyplot as plt
import io
import base64

# ==================== DEFINE NODE ====================

nodeio.context(
    name="FinalDataAnalysis",
    description="Final fitting and plotting",
    icon="bootstrap/file-bar-graph.svg",
)

input = nodeio.Inputs()
input.stream("data", units="JSON", description="experiment results")

output = nodeio.Outputs()
output.define(
    "estimated_parameter", units="MHz", description="final result", retention=2
)
output.define("final_plot", units="png", description="to-do", retention=2)

nodeio.register()  # enables this to be used as part of workflow


# ==================== DRY-RUN DATA ====================

input.set(data=3.23)

# =============== RUN NODE STATE MACHINE ===============

while nodeio.status.active:
    a = input.get("data")
    print(a)

    # do some custom plot
    x = np.linspace(0, 10, 100)
    y = np.cos(a * x / 10)
    plt.figure()
    plt.plot(x, y, "b-")
    plt.plot(x, np.cos(y) / 3, "r-")
    plt.xlabel("Time (s)")
    plt.ylabel("Amplitude (rel.un)")
    plt.legend(["probe1", "drive1"])

    buffer = io.BytesIO()

    plt.savefig(buffer, format="png")

    buffer.seek(0)
    figure_base64_encoded_png = base64.b64encode(buffer.read()).decode()

    output.set(estimated_parameter=y.tolist())
    output.set(final_plot={"png": figure_base64_encoded_png})


# ================= =================== =================

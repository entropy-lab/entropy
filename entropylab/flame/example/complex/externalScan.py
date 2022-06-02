import entropylab.flame.nodeio as nodeio

# ==================== DEFINE NODE ====================

nodeio.context(
    name="ExternalScan",
    description="Sets and maintains instrument variables",
    icon="bootstrap/sliders.svg",
)

input = nodeio.Inputs()
input.stream(
    "set_point",
    units="status",
    description="node will execute only if it receives positive calibration status",
)

output = nodeio.Outputs()
output.define("setpoint_reached", units="bool", description="Trigger", retention=0)

nodeio.register()  # enables this to be used as part of workflow

# ==================== DRY-RUN DATA ====================

input.set(set_point=3.2)

# =============== RUN NODE STATE MACHINE ===============

while nodeio.status.active:
    target = input.get("set_point")

    print(f"calibrating, target = {target}")

    # time.sleep(0.01)
    output.set(setpoint_reached=True)

# ================= =================== =================

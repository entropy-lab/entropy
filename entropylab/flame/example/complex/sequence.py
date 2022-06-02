import entropylab.flame.nodeio as nodeio
import time

# ==================== DEFINE NODE ====================

nodeio.context(
    name="TaskList",
    description="Does scan of parameters and triggers circuit execution",
    icon="bootstrap/list-task.svg",
)

input = nodeio.Inputs()
input.state(
    "calibration",
    units="status",
    description="node will execute only if it receives positive calibration status",
)
input.state("scan_points", units="MHz", description="an array for setpoint execution")

input.stream(
    "laser_setpoint_locked",
    units="bool",
    description="Triger when laser setpoint is set",
)
input.stream(
    "circuit_done", units="bool", description="Triger when circuit execution is done"
)

output = nodeio.Outputs()
output.define(
    "circuit_specification",
    units="JSON",
    description="Triggers circuit execution by providing information for run",
    retention=0,
)
output.define("scan_setpoint", units="MHz", description="Laser setpoint")
output.define(
    "status", units="json", description="window into what node is doing", retention=1
)

nodeio.register()  # enables this to be used as part of workflow

# ==================== DRY-RUN DATA ====================

input.set(scan_points=[1, 2, 3], calibration="calibrated")
input.set(laser_setpoint_locked=True, circuit_done=True)
input.set(laser_setpoint_locked=True, circuit_done=True)
input.set(laser_setpoint_locked=True, circuit_done=True)

# =============== RUN NODE STATE MACHINE ===============

sequence = input.get("scan_points")
index = 0

while nodeio.status.active:
    status = input.get("calibration")
    if status == "calibrated":
        if index < len(sequence):
            output.set(status={"scan point": sequence[index]})
            print("Set point : ", sequence[index])
            output.set(scan_setpoint=sequence[index])
            setpointReached = input.get("laser_setpoint_locked")
            output.set(circuit_specification=sequence[index])
            circuit_done = input.get("circuit_done")
            time.sleep(2)
        else:
            print("terminating experiment...")
            time.sleep(2)
            nodeio.terminate_workflow()
        index += 1
    else:
        pass


# ================= =================== =================

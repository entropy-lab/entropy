import entropylab.flame.nodeio as nodeio
import time

# ==================== DEFINE NODE ====================

nodeio.context(
    name="CalibrateSystem",
    description="Makes sure that system is calibrated",
    icon="bootstrap/gear-fill.svg",
)

input = nodeio.Inputs()
input.state(
    "instrumentAddress",
    units="IP address",
    description="instrument IP address for calibration",
)

output = nodeio.Outputs()
output.define(
    "calibration_status",
    units="calibrated, uncalibrated",
    description="Calibration status",
    retention=0,
)

nodeio.register()  # enables this to be used as part of workflow

# =============== RUN NODE STATE MACHINE ===============

while nodeio.status.active:
    a = False
    print("calibrating")

    time.sleep(0.5)
    new_state = True

    if new_state:
        if new_state != a:
            output.set(calibration_status="calibrated")
    else:
        if new_state != a:
            output.set(calibration_status="uncalibrated")
    a = new_state

# ================= =================== =================

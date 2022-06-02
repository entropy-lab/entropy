import entropynodes.library as expNodes
from entropylab.flame.workflow import Workflow

wf = Workflow(
    "Scan with error correction",
    description="Performs Bayesian estimation for the optimal parameters "
    "and uses this during the circuit run.",
)

c = expNodes.CalibrateSystem("calibration_1")

scan_list = expNodes.TaskList(
    "scan_list",
    calibration=c.o.calibration_status,
)

laser = expNodes.ExternalScan(
    "external_instruments", set_point=scan_list.o.scan_setpoint
)
scan_list.i.laser_setpoint_locked = laser.o.setpoint_reached

qpu = expNodes.QPUcircuitRunner("QPU", circuit_param=scan_list.o.circuit_specification)
scan_list.i.circuit_done = qpu.o.circuit_finished

errCor = expNodes.BayesianEstimation("correction_1")
errCor.i.precorrected_data = qpu.o.precorrected_data
qpu.i.error_correction = errCor.o.correction_data

final_report = expNodes.FinalDataAnalysis("final_report", data=qpu.o.final_data)

wf.register()

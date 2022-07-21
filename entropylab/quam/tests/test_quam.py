from entropylab.quam.core import QuAMManager
from qualang_tools.config.components import *
from qualang_tools.config.primitive_components import *
from qualang_tools.config import ConfigBuilder

from qm.qua import *
import pytest


class MyManager(QuAMManager):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def prepare_config(self, cb: ConfigBuilder):
        con1 = Controller("con1")
        cb.add(con1)
        qb = Transmon(
            "qb",
            I=con1.analog_output(1),
            Q=con1.analog_output(2),
            intermediate_frequency=1e7,
        )
        cb.add(qb)
        qb.lo_frequency = self.parameter("lo")
        qb.add(
            ControlPulse(
                "cw",
                [
                    ConstantWaveform("const_wf", 0.1),
                    ConstantWaveform("zero", 0),
                ],
                20,
            )
        )


@pytest.mark.skip(reason="requires a gateway server")
def test_quam(db_file_path):
    quam = MyManager(path=db_file_path)
    quam.param_store["lo"] = 5e5
    quam.param_store.save_temp()

    def voltage_setter(val: float):
        print(val)

    voltage = quam.parameter("voltage", setter=voltage_setter)
    voltage(12)

    print(quam.generate_config())

    print(quam.elements.qb)

    with program() as prog:
        play(quam.pulses.cw, quam.elements.qb)

    from qm.simulate import SimulationConfig

    res = quam.open_qm().simulate(prog, simulate=SimulationConfig(duration=1000))

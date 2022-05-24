from entropylab.quam.core import Admin
from qualang_tools.config.components import *
from qualang_tools.config.primitive_components import *
from qualang_tools.config import ConfigBuilder

from qm.qua import *


class MyAdmin(Admin):
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


path = "tests_cache/entropy.db"


def test_admin(path):
    admin = MyAdmin(path=path)
    admin.set(lo=5e5)
    admin.params.save_temp()
    print(admin.config)


def test_oracle(path):
    admin = MyAdmin(path=path)
    admin.params.load_temp()
    oracle = admin.get_oracle()
    # print(oracle.params.list_commits())
    print(oracle.elements)
    print(oracle.parameters)
    print(oracle.pulses)


def test_user(path):
    admin = MyAdmin(path=path)
    admin.params.load_temp()
    user = admin.get_user()
    print(user.elements.qb)

    with program() as prog:
        play(user.pulses.cw, user.elements.qb)

    res = user.simulate(prog)

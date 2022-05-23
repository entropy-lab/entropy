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


admin = MyAdmin(path="entropy.db")
admin.set(lo=5e5)
admin.params.commit("init")
print(admin.config)

oracle = admin.get_oracle()
print(oracle.params.list_commits())
print(oracle.elements)
oracle.params.checkout("566d691a7fe492c12e3ae53f61f18e162d788101")
print(oracle.parameters)
print(oracle.pulses)

user = admin.get_user()
print(user.elements.qb)

with program() as prog:
    play(user.pulses.cw, user.elements.qb)

res = user.simulate(prog)
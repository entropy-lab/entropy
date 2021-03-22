from quaentropy.instruments.instrument_driver import Instrument


class MockScope(Instrument):
    def __init__(self, name: str):
        super().__init__(name)
        self.index = 0

    def setup_driver(self):
        pass

    def teardown_driver(self):
        pass

    def discover_driver_specs(self):
        super().discover_driver_specs()

    def snapshot(self, update: bool):
        pass

    def get_trig(self):
        self.index += 1
        print(f"got trig {self.index}")

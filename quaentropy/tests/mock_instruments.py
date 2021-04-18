from quaentropy.instruments.instrument_driver import Instrument


class MockScope(Instrument):
    def __init__(self, address: str, extra: str, **kwargs):
        super().__init__(**kwargs)
        self.index = 0
        self.address = address
        self.extra = extra

    def setup_driver(self):
        pass

    def teardown_driver(self):
        pass

    def discover_driver_specs(self):
        super().discover_driver_specs()

    def get_trig(self):
        self.index += 1
        print(f"got trig {self.index}")

    def snapshot(self, update: bool) -> str:
        return str(self.index)

    def revert_to_snapshot(self, snapshot: str):
        self.index = int(snapshot)

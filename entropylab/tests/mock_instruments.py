from entropylab.components.instrument_driver import Resource


class MockScope(Resource):
    def __init__(self, address: str, extra: str, **kwargs):
        super().__init__(**kwargs)
        self.index = 0
        self.address = address
        self.extra = extra

    def connect(self):
        pass

    def teardown(self):
        pass

    def get_trig(self):
        self.index += 1
        print(f"got trig {self.index}")

    def snapshot(self, update: bool) -> str:
        return str(self.index)

    def revert_to_snapshot(self, snapshot: str):
        self.index = int(snapshot)

from entropylab.pipeline.params.persistence.persistence import Metadata


def test_metadata___repr__():
    target = Metadata("foo", 1658997559516187400, "bar")
    assert (
        target.__repr__()
        == "Metadata(id='foo', timestamp=1658997559516187400, label='bar')"
    )

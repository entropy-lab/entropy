import entropylab.flame.nodeio as nodeio


def test_stream_and_state_inputs():
    nodeio.context(
        name="TestNode",
        description="test",
        icon="bootstrap/person-circle.svg",
    )

    inputs = nodeio.Inputs()
    inputs.stream("stream_1", units="test", description="desc")
    inputs.state("state_1", units="test", description="desc")

    nodeio.register()

    # ==================== DRY RUN DATA ====================

    inputs.set(state_1="sunny")
    inputs.set(state_1="sunny")
    inputs.set(state_1="rainy")
    inputs.set(stream_1="Alice")
    inputs.set(stream_1="Bob")
    inputs.set(stream_1="Mars")
    inputs.set(stream_1="Venus")

    assert inputs.get("state_1") == "sunny"
    assert inputs.get("state_1") == "sunny"
    assert inputs.get("state_1") == "rainy"
    assert inputs.get("state_1") == "rainy"
    assert inputs.get("state_1") == "rainy"

    assert inputs.get("stream_1") == "Alice"
    assert inputs.get("stream_1") == "Bob"
    assert inputs.get("stream_1") == "Mars"
    assert inputs.get("stream_1") == "Venus"

from entropylab import PyNode, Graph
from entropylab.api.graph import RetryBehavior
from entropylab.logger import logger

counter = 1


def a():
    global counter
    if counter < 3:
        counter = counter + 1
        raise Exception("still not working")
    counter = 1
    return {"x": 1}


def test_retry_behavior(caplog):
    a1 = PyNode(
        "a1",
        a,
        output_vars={"x"},
        retry_on_error=RetryBehavior(backoff=2, wait_time=0.2),
    )
    a2 = PyNode(
        "a2",
        a,
        output_vars={"x"},
        must_run_after={a1},
        retry_on_error=RetryBehavior(added_delay=0.1, wait_time=0.2, backoff=1),
    )
    handle = Graph(None, {a1, a2}, "must_run_after").run()
    assert len(list(handle.results.get_results_from_node("a1"))) == 1
    assert len(list(handle.results.get_results_from_node("a2"))) == 1
    # assert (
    #     list(list(handle.results.get_results_from_node("a2"))[0].results)[0].label
    #     == "x"
    # )
    # assert (
    #     "node a1 has error, retrying #2 in 0.4 seconds : still not working"
    #     in caplog.text
    # )
    # assert "node a2 has error, retrying #2 in 0.3" in caplog.text

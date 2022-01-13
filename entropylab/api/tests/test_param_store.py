from entropylab.api.param_store import InProcessParamStore


def test_in_process_set_and_get():
    target = InProcessParamStore()
    target["foo"] = "bar"
    assert target["foo"] == "bar"


def test_commit_empty_dict():
    target = InProcessParamStore()
    assert target.commit == "bf21a9e8fbc5a3846fb05b4fa0859e0917b2202f"


def test__generate_id_empty_dict():
    target = InProcessParamStore()
    assert target._generate_id() == "bf21a9e8fbc5a3846fb05b4fa0859e0917b2202f"


def test__generate_id_nonempty_dict():
    target = InProcessParamStore()
    target["foo"] = "bar"
    assert target._generate_id() == "bc4919c6adf7168088eaea06e27a5b23f0f9f9da"

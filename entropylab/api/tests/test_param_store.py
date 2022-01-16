import pytest

from entropylab.api.param_store import InProcessParamStore

""" __getitem()__"""


def test_get_when_key_is_present_then_value_is_returned():
    target = InProcessParamStore()
    target["foo"] = "bar"
    assert target["foo"] == "bar"


def test_get_when_key_is_missing_then_keyerror_is_raised():
    target = InProcessParamStore()
    with pytest.raises(KeyError):
        # noinspection PyStatementEffect
        target["foo"]


def test_get_when_key_is_none_then_keyerror_is_raised():
    target = InProcessParamStore()
    with pytest.raises(KeyError):
        # noinspection PyTypeChecker,PyStatementEffect
        target[None]


""" commit() """


def test_commit_when_body_is_empty_does_not_throw(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    assert target.commit() == "bf21a9e8fbc5a3846fb05b4fa0859e0917b2202f"


def test_commit_when_committing_twice_the_same_id_is_returned(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    first = target.commit()
    second = target.commit()
    assert first == second


def test_commit_when_committing_same_state_twice_the_same_id_is_returned(
    tinydb_file_path,
):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    first = target.commit()
    del target["foo"]
    # noinspection PyUnusedLocal
    second = target.commit()
    target["foo"] = "bar"
    # act
    third = target.commit()
    # assert
    assert first == third


""" checkout() """


def test_checkout_and_id_removed_from_dict(tinydb_file_path):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    commit_id = target.commit()
    target["foo"] = "baz"
    # act
    target.checkout(commit_id)
    # assert
    assert target["foo"] == "bar"
    assert "_id" not in target


""" _hash_dict() """


def test__hash_dict_empty_dict():
    target = InProcessParamStore()
    assert target._hash_dict() == "bf21a9e8fbc5a3846fb05b4fa0859e0917b2202f"


def test__hash_dict_nonempty_dict():
    target = InProcessParamStore()
    target["foo"] = "bar"
    assert target._hash_dict() == "bc4919c6adf7168088eaea06e27a5b23f0f9f9da"

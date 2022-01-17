import pytest

from entropylab.api.param_store import InProcessParamStore

""" __getitem()__"""


def test___getattribute_works():
    target = InProcessParamStore()
    target["foo"] = "bar"
    assert target.foo == "bar"


def test___setattr_works():
    target = InProcessParamStore()
    target.foo = "bar"
    assert target["foo"] == "bar"


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
    assert len(target.commit()) == 40


def test_commit_when_committing_non_dirty_does_nothing(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    first = target.commit()
    second = target.commit()
    assert first == second


def test_commit_when_committing_same_state_twice_a_different_id_is_returned(
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
    assert first != third


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


""" _generate_header() """


def test__generate_header_empty_dict():
    target = InProcessParamStore()
    actual = target._generate_header()
    assert len(actual.id) == 40


def test__generate_header_nonempty_dict():
    target = InProcessParamStore()
    target["foo"] = "bar"
    actual = target._generate_header()
    assert len(actual.id) == 40
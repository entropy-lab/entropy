import pytest
from tinydb import Query

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


def test_commit_when_label_is_not_given_then_null_is_saved(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    commit_id = target.commit()
    result = target._db.search(Query().metadata.id == commit_id)
    assert result[0]["metadata"]["label"] is None


def test_commit_when_label_is_given_then_label_is_saved(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    commit_id = target.commit("foo")
    result = target._db.search(Query().metadata.id == commit_id)
    assert result[0]["metadata"]["label"] == "foo"


""" checkout() """


def test_checkout_when_commit_id_exists_value_is_reverted(tinydb_file_path):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    commit_id = target.commit()
    target["foo"] = "baz"
    # act
    target.checkout(commit_id)
    # assert
    assert target["foo"] == "bar"


def test_checkout_when_commit_id_exists_value_remains_the_same(tinydb_file_path):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    commit_id = target.commit()
    # act
    target.checkout(commit_id)
    # assert
    assert target["foo"] == "bar"


def test_checkout_when_commit_id_exists_value_is_removed(tinydb_file_path):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    commit_id = target.commit()
    target["foo"] = "baz"
    # act
    target.checkout(commit_id)
    # assert
    assert "foo" not in target


""" _generate_metadata() """


def test__generate_metadata_empty_dict():
    target = InProcessParamStore()
    actual = target._generate_metadata()
    assert len(actual.id) == 40


def test__generate_metadata_nonempty_dict():
    target = InProcessParamStore()
    target["foo"] = "bar"
    actual = target._generate_metadata()
    assert len(actual.id) == 40

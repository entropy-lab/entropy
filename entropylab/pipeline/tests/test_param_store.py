import time
from datetime import datetime
from datetime import timedelta
from pprint import pprint
from time import sleep

import pandas as pd
import pytest
from tinydb import TinyDB

from entropylab.conftest import _copy_template, Process
from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.api.in_process_param_store import (
    InProcessParamStore,
    MergeStrategy,
)
from entropylab.pipeline.api.param_store import Param, LOCAL_TZ, _ns_to_datetime
from entropylab.pipeline.params.persistence.migrations import fix_param_qualified_name, \
    migrate_param_store_0_1_to_0_2
from entropylab.pipeline.params.persistence.persistence import Metadata
from entropylab.pipeline.params.persistence.tinydb.storage import JSONPickleStorage
from entropylab.pipeline.params.persistence.tinydb.tinydbpersistence import (
    _set_version,
)

""" ctor """


def test_ctor_when_store_is_empty_then_is_dirty_is_false():
    target = InProcessParamStore()
    assert target.is_dirty is False


def test_ctor_when_store_is_empty_then_latest_commit_is_checked_out(tinydb_file_path):
    # arrange
    with InProcessParamStore(tinydb_file_path) as param_store:
        param_store.foo = "bar"
        param_store.commit()
    # act
    target = InProcessParamStore(tinydb_file_path)
    # assert
    assert target.foo == "bar"


""" MutableMapping """


def test___iter___works():
    target = InProcessParamStore()
    target["foo"] = "bar"
    target["baz"] = "buzz"
    actual = iter(target)
    assert next(actual) == "foo"
    assert next(actual) == "baz"


def test___len___works():
    target = InProcessParamStore()
    target["foo"] = "bar"
    target["baz"] = "buzz"
    assert len(target) == 2


def test___contains___works():
    target = InProcessParamStore()
    target["foo"] = "bar"
    assert "foo" in target


def test___getattr___works():
    target = InProcessParamStore()
    target["foo"] = "bar"
    assert target.foo == "bar"


def test___setattr___works():
    target = InProcessParamStore()
    target.foo = "bar"
    assert target["foo"] == "bar"


def test___getitem___when_key_is_present_then_value_is_returned():
    target = InProcessParamStore()
    target["foo"] = "bar"
    assert target["foo"] == "bar"


def test___getitem___when_key_is_missing_then_keyerror_is_raised():
    target = InProcessParamStore()
    with pytest.raises(KeyError):
        # noinspection PyStatementEffect
        target["foo"]


def test___getitem___when_key_is_none_then_keyerror_is_raised():
    target = InProcessParamStore()
    with pytest.raises(KeyError):
        # noinspection PyTypeChecker,PyStatementEffect
        target[None]


def test___getitem___when_key_starts_with_underscore_then_keyerror_is_raised():
    target = InProcessParamStore()
    with pytest.raises(KeyError):
        # noinspection PyTypeChecker,PyStatementEffect
        target["_base_doc_id"]


def test___setitem___when_key_starts_with_underscore_then_key_can_be_retrieved():
    target = InProcessParamStore()
    target["_base_doc_id"] = "bar"
    bar = target["_base_doc_id"]
    assert bar == "bar"


def test___setitem___when_key_starts_with_dunder_then_key_is_not_saved_to_db(
    tinydb_file_path,
):
    with InProcessParamStore(tinydb_file_path) as target:
        target.__foo = "bar"
        target.foo = "baz"
        target.commit()
    with open(tinydb_file_path) as f:
        assert "__foo" not in f.read()


def test___setitem___when_saving_dict_then_only_entire_dict_is_wrapped_in_param(
    tinydb_file_path,
):
    with InProcessParamStore(tinydb_file_path) as target:
        target.foo = dict(bar="baz")
        target.commit()
    db = TinyDB(tinydb_file_path, storage=JSONPickleStorage)
    doc = db.all()[0]
    assert doc["params"]["foo"] == Param(dict(bar="baz"))


def test___delitem__():
    target = InProcessParamStore()
    target["foo"] = "bar"
    del target["foo"]
    assert "foo" not in target


def test___delitem___when_key_is_deleted_then_it_is_removed_from_tags_too():
    target = InProcessParamStore()
    target["foo"] = "bar"
    target["goo"] = "baz"
    target.add_tag("tag", "foo")
    target.add_tag("tag", "goo")
    # act
    del target["foo"]
    assert target.list_keys_for_tag("tag") == ["goo"]


def test___repr__():
    target = InProcessParamStore()
    target["foo"] = "bar"
    actual = target.__repr__()
    assert actual == "<InProcessParamStore({'foo': 'bar'})>"


""" get() """


def test_get_when_commit_id_is_none_then_value_is_returned():
    # arrange
    target = InProcessParamStore()
    target["foo"] = "bar"
    # act
    actual = target.get_value("foo")
    # assert
    assert actual == "bar"


def test_get_when_commit_id_is_not_none_then_value_is_returned():
    # arrange
    target = InProcessParamStore()
    target["foo"] = "bar"
    commit_id = target.commit()
    target["foo"] = "baz"
    # act
    actual = target.get_value("foo", commit_id)
    # assert
    assert actual == "bar"


def test_get_when_commit_id_is_bad_then_entropy_error_is_raised():
    # arrange
    target = InProcessParamStore()
    target["foo"] = "bar"
    # act
    with pytest.raises(EntropyError):
        target.get_value("foo", "oops")


""" get_param() """


def test_get_param_when_param_exists_then_it_is_returned():
    # arrange
    target = InProcessParamStore()
    target["foo"] = "bar"
    # act
    actual = target.get_param("foo")
    # assert
    assert actual.value == "bar"
    assert actual.commit_id is None


def test_get_param_when_param_is_committed_then_commit_id_is_returned_in_param():
    # arrange
    target = InProcessParamStore()
    target["foo"] = "bar"
    commit_id = target.commit()
    # act
    actual = target.get_param("foo")
    # assert
    assert actual.commit_id == commit_id


def test_get_param_when_key_is_not_in_param_store_then_keyerror_is_raised():
    target = InProcessParamStore()
    with pytest.raises(KeyError):
        target.get_param("foo")


def test_get_param_when_commit_id_is_not_none_then_value_is_returned():
    # arrange
    target = InProcessParamStore()
    target["foo"] = "bar"
    commit_id = target.commit()
    target["foo"] = "baz"
    # act
    actual = target.get_param("foo", commit_id)
    # assert
    assert actual.value == "bar"


def test_get_param_when_key_is_not_in_commit_then_keyerror_is_raised():
    # arrange
    target = InProcessParamStore()
    target["foo"] = "bar"
    commit_id = target.commit()
    # act & assert
    with pytest.raises(KeyError):
        target.get_param("oops", commit_id)


def test_get_param_when_commit_id_is_bad_then_entropy_error_is_raised():
    # arrange
    target = InProcessParamStore()
    target["foo"] = "bar"
    # act
    with pytest.raises(EntropyError):
        target.get_param("foo", "oops")


""" set_param() """


def test_set_param_when_param_is_new_then_value_is_set():
    # arrange
    target = InProcessParamStore()
    # act
    target.set_param("foo", "bar")
    # assert
    assert target["foo"] == "bar"


def test_set_param_when_param_exists_then_value_is_overwritten():
    # arrange
    target = InProcessParamStore()
    target.set_param("foo", "bar")
    # act
    target.set_param("foo", "baz")
    # assert
    assert target["foo"] == "baz"


def test_set_param_when_commit_id_is_in_kwargs_then_value_error_is_raised():
    # arrange
    target = InProcessParamStore()
    with pytest.raises(ValueError):
        target.set_param("foo", "bar", commit_id="oops")


@pytest.mark.parametrize(
    "attr, value",
    [
        ("expiration", timedelta(seconds=1337)),
        ("expiration", None),
        ("description", "buzz"),
        ("description", None),
        ("node_id", "1"),
        ("node_id", None),
    ],
)
def test_set_param_when_an_attribute_is_in_kwargs_then_it_is_set(attr, value):
    # arrange
    target = InProcessParamStore()
    target.set_param(
        "foo", "bar", expiration=timedelta(seconds=42), description="baz", node_id=0
    )
    # act
    target.set_param("foo", "bar", **{attr: value})
    # assert
    actual = target.get_param("foo")
    assert getattr(actual, attr) == value


""" rename_key() """


def test_rename_key_when_key_exists_then_it_is_renamed():
    # arrange
    target = InProcessParamStore()
    target["foo"] = dict(bar="baz")
    # act
    target.rename_key("foo", "new")
    # assert
    assert target["new"]["bar"] == "baz"


def test_rename_key_when_key_does_not_exist_then_an_error_is_raised():
    target = InProcessParamStore()
    with pytest.raises(KeyError):
        target.rename_key("foo", "new")


def test_rename_key_when_new_key_exists_then_an_error_is_raised():
    # arrange
    target = InProcessParamStore()
    target["foo"] = dict(bar="baz")
    target["new"] = 42

    # act & assert
    with pytest.raises(KeyError):
        target.rename_key("foo", "new")


def test_rename_key_when_key_has_a_tag_then_tag_remains():
    # arrange
    target = InProcessParamStore()
    target["foo"] = dict(bar="baz")
    target.add_tag("tag", "foo")
    # act
    target.rename_key("foo", "new")
    # assert
    assert "tag" in target.list_tags_for_key("new")
    assert "tag" not in target.list_tags_for_key("foo")


""" diff() """


def test_diff_existing_value_changed():
    target = InProcessParamStore()
    target.foo = "bar"
    target.commit()
    target.foo = "baz"
    actual = target.diff()
    assert actual == {"foo": {"old_value": "bar", "new_value": "baz"}}


def test_diff_existing_value_changed_and_changed_back():
    target = InProcessParamStore()
    target.foo = "bar"
    target.commit()
    target.foo = "baz"
    target.foo = "bar"
    actual = target.diff()
    assert actual == {}


def test_diff_existing_value_deleted():
    target = InProcessParamStore()
    target.foo = "bar"
    target.commit()
    del target["foo"]
    actual = target.diff()
    assert actual == {"foo": {"old_value": "bar"}}


def test_diff_new_value_added():
    target = InProcessParamStore()
    target.foo = "bar"
    target.commit()
    target.boo = "baz"
    actual = target.diff()
    assert actual == {"boo": {"new_value": "baz"}}


def test_diff__no_previous_commit_new_value_added():
    target = InProcessParamStore()
    target.foo = "bar"
    actual = target.diff()
    assert actual == {"foo": {"new_value": "bar"}}


def test_diff__no_previous_commit_new_value_added_then_removed():
    target = InProcessParamStore()
    target.foo = "bar"
    del target["foo"]
    actual = target.diff()
    assert actual == {}


def test_diff_when_commit_ids_are_given_then_they_are_used():
    target = InProcessParamStore()
    target.foo = "bar"
    first = target.commit()
    target.foo = "baz"
    second = target.commit()
    target.foo = "buzz"
    actual = target.diff(first, second)
    assert actual == {"foo": {"old_value": "bar", "new_value": "baz"}}


def test_diff_when_commit_ids_are_used_in_reverse_then_result_is_reversed():
    target = InProcessParamStore()
    target.foo = "bar"
    first = target.commit()
    target.foo = "baz"
    second = target.commit()
    target.foo = "buzz"
    actual = target.diff(second, first)
    assert actual == {"foo": {"old_value": "baz", "new_value": "bar"}}


""" commit() """


def test_commit_in_memory_when_param_changes_commit_doesnt_change():
    # arrange
    target = InProcessParamStore()
    target["foo"] = "bar"
    commit_id = target.commit()
    # act
    target["foo"] = "baz"
    # assert
    assert target.get_value("foo", commit_id) == "bar"


def test_commit_when_body_is_empty_does_not_throw(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    target.foo = "bar"
    assert len(target.commit()) == 40


def test_commit_when_committing_non_dirty_then_new_commit_id_is_creat(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    first = target.commit()
    second = target.commit()
    assert first != second


def test_commit_when_committing_same_state_twice_a_different_id_is_returned(
    tinydb_file_path,
):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    first = target.commit()
    sleep(0.1)
    del target["foo"]
    # noinspection PyUnusedLocal
    second = target.commit()
    target["foo"] = "bar"
    # act
    third = target.commit()
    # assert
    assert first != third


def test_commit_when_label_is_not_given_then_null_label_is_saved(tinydb_file_path):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target.foo = "bar"
    # act
    target.commit()
    # assert
    commits = target.list_commits()
    assert commits[0].label is None


def test_commit_when_label_is_given_then_label_is_saved(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    target.foo = "bar"
    target.commit("baz")
    # assert
    commits = target.list_commits()
    assert commits[0].label == "baz"


def test_commit_assert_changed_values_are_stamped_with_commit_id(tinydb_file_path):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = 42
    target["bar"] = 42
    commit_id1 = target.commit()
    target["bar"] = 1337
    target["baz"] = 1337
    # act
    commit_id2 = target.commit()
    # assert
    assert target._InProcessParamStore__params["foo"].commit_id == commit_id1
    assert target._InProcessParamStore__params["bar"].commit_id == commit_id2
    assert target._InProcessParamStore__params["baz"].commit_id == commit_id2


def test_commit_assert_param_expiration_is_converted_to_timestamp_int(tinydb_file_path):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    now = time.time_ns()
    target.set_param("foo", 42, expiration=timedelta(hours=5))
    # act
    target.commit()
    # assert
    assert target.get_param("foo").expiration >= now + (5 * 1e9)


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
    target["foo"] = "bar"
    commit_id = target.commit()
    target["baz"] = "buzz"
    # act
    target.checkout(commit_id)
    # assert
    assert "baz" not in target


def test_checkout_when_commit_id_doesnt_exist_then_error_is_raised(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    with pytest.raises(EntropyError):
        target.checkout("foo")


def test_checkout_when_commit_num_exists_value_is_reverted(tinydb_file_path):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    commit_id = target.commit()
    target["foo"] = "baz"
    # act
    target.checkout(commit_num=1)
    # assert
    assert target["foo"] == "bar"


def test_checkout_when_tag_existed_in_commit_then_it_is_added_to_store(
    tinydb_file_path,
):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    target.add_tag("tag", "foo")
    commit_id = target.commit()
    target.remove_tag("tag", "foo")
    target.checkout(commit_id)
    assert target.list_keys_for_tag("tag") == ["foo"]


def test_checkout_when_tag_did_not_exist_in_commit_then_it_is_removed_from_store(
    tinydb_file_path,
):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    commit_id = target.commit()
    target.add_tag("tag", "foo")
    # act
    target.checkout(commit_id)
    # assert
    assert target.list_keys_for_tag("tag") == []


def test_checkout_when_no_args_then_latest_commit_is_checked_out(
    tinydb_file_path,
):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    commit_id = target.commit()
    target["foo"] = "baz"
    target.commit()
    target.checkout(commit_id=commit_id)
    # act
    target.checkout()
    # assert
    assert target["foo"] == "baz"


def test_checkout_when_no_args_and_no_commits_then_nothing_happens(
    tinydb_file_path,
):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    target.checkout()
    assert target["foo"] == "bar"


@pytest.mark.parametrize(
    "commit_num",
    [2, 0, -1],
)
def test_checkout_when_commit_num_doesnt_exist_error_is_raised(
    tinydb_file_path, commit_num
):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    target.commit()  # commit_num == 1
    # act & assert
    with pytest.raises(EntropyError):
        target.checkout(commit_num=commit_num)


""" list_commits() """


def test_list_commits_no_args_returns_all_metadata(
    tinydb_file_path,
):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    target.commit("first")
    target["foo"] = "baz"
    target.commit("second")
    target["foo"] = "buzz"
    target.commit("third")
    # act
    actual = target.list_commits()
    # assert
    # assert all(type(m) == Metadata for m in actual)
    assert actual[0].label == "first"
    assert actual[1].label == "second"
    assert actual[2].label == "third"


def test_list_commits_when_label_exists_then_it_is_returned(
    tinydb_file_path,
):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "exact"
    target.commit("label")
    target["foo"] = "pre"
    target.commit("foo-label")
    target["foo"] = "post"
    target.commit("label-foo")
    target["foo"] = "no-match"
    target.commit("foo")
    target["foo"] = "empty"
    target.commit("")
    target["foo"] = "None"
    target.commit()
    # act
    actual = target.list_commits("label")
    # assert
    assert all(type(m) == Metadata for m in actual)
    assert all("label" in m.label for m in actual)
    assert len(actual) == 1


""" merge() MergeStrategy.OURS """


def test_merge_strategy_ours_when_both_are_empty_result_is_empty():
    target = InProcessParamStore()
    theirs = InProcessParamStore()
    target.merge(theirs, MergeStrategy.OURS)
    assert len(target.to_dict().items()) == 0


def test_merge_strategy_ours_when_their_key_is_new_then_it_is_copied():
    target = InProcessParamStore()
    theirs = InProcessParamStore()
    theirs["foo"] = "bar"
    target.merge(theirs, MergeStrategy.OURS)
    assert target["foo"] == "bar"


def test_merge_strategy_ours_when_their_key_is_present_in_ours_then_it_is_ignored():
    target = InProcessParamStore()
    target["foo"] = "bar"
    theirs = InProcessParamStore()
    theirs["foo"] = "baz"
    target.merge(theirs, MergeStrategy.OURS)
    assert target["foo"] == "bar"


def test_merge_strategy_ours_merge_two_leaves_under_same_parent_dict():
    target = InProcessParamStore()
    target["foo"] = {"a": 1}
    theirs = InProcessParamStore()
    theirs["foo"] = {"b": 2}
    target.merge(theirs, MergeStrategy.OURS)
    assert target["foo"]["a"] == 1 and target["foo"]["b"] == 2


def test_merge_strategy_ours_when_ours_is_leaf_theirs_is_dict_then_theirs_is_ignored():
    target = InProcessParamStore()
    target["foo"] = "bar"
    theirs = InProcessParamStore()
    theirs["foo"] = {"baz": 1}
    target.merge(theirs, MergeStrategy.OURS)
    assert target["foo"] == "bar"


def test_merge_strategy_ours_when_ours_is_dict_theirs_is_leaf_then_theirs_is_ignored():
    target = InProcessParamStore()
    target["foo"] = {"bar": 1}
    theirs = InProcessParamStore()
    theirs["foo"] = "baz"
    target.merge(theirs, MergeStrategy.OURS)
    assert target["foo"] == {"bar": 1}


def test_merge_strategy_ours_both_sides():
    target = InProcessParamStore()
    target["foo"] = dict(a=1, b=dict(y=5, z=6))
    theirs = InProcessParamStore()
    theirs["foo"] = dict(b=dict(x=4, y=-5), c=3)
    target.merge(theirs, MergeStrategy.OURS)
    assert target.to_dict() == {
        "foo": {
            "a": 1,
            "b": {"x": 4, "y": 5, "z": 6},
            "c": 3,
        }
    }


def test_merge_strategy_ours_correctly_marks_key_for_dict_as_dirty():
    # arrange
    target = InProcessParamStore()
    target["foo"] = {"x": {"a": 1}}
    theirs = InProcessParamStore()
    theirs["foo"] = {"x": {"b": 2}}
    target.commit()  # so we start the merge in  a non-dirty state
    # act
    target.merge(theirs, MergeStrategy.OURS)
    # assert
    assert target._InProcessParamStore__dirty_keys == {"foo"}
    assert target["foo"]["x"]["a"] == 1 and target["foo"]["x"]["b"] == 2


def test_merge_strategy_ours_when_both_are_empty_then_store_remains_not_dirty():
    target = InProcessParamStore()
    theirs = InProcessParamStore()
    target.commit()  # so is_dirty becomes False
    target.merge(theirs, MergeStrategy.OURS)
    assert not target.is_dirty


def test_merge_strategy_ours_when_their_key_is_copied_then_store_is_dirty():
    target = InProcessParamStore()
    theirs = InProcessParamStore()
    theirs["foo"] = "bar"
    target.commit()  # so is_dirty becomes False
    target.merge(theirs, MergeStrategy.OURS)
    assert target.is_dirty


""" merge() MergeStrategy.THEIRS """


def test_merge_strategy_theirs_when_both_are_empty_result_is_empty():
    target = InProcessParamStore()
    theirs = InProcessParamStore()
    target.merge(theirs, MergeStrategy.THEIRS)
    assert len(target.to_dict().items()) == 0


def test_merge_strategy_theirs_when_their_key_is_new_then_it_is_copied():
    target = InProcessParamStore()
    theirs = InProcessParamStore()
    theirs["foo"] = "bar"
    target.merge(theirs, MergeStrategy.THEIRS)
    assert target["foo"] == "bar"


def test_merge_strategy_theirs_when_their_key_is_present_in_ours_then_it_overwrites():
    target = InProcessParamStore()
    target["foo"] = "bar"
    theirs = InProcessParamStore()
    theirs["foo"] = "baz"
    target.merge(theirs, MergeStrategy.THEIRS)
    assert target["foo"] == "baz"


def test_merge_strategy_theirs_merge_two_leaves_under_same_parent_dict():
    target = InProcessParamStore()
    target["foo"] = {"a": 1}
    theirs = InProcessParamStore()
    theirs["foo"] = {"b": 2}
    target.merge(theirs, MergeStrategy.THEIRS)
    assert target["foo"]["a"] == 1 and target["foo"]["b"] == 2


def test_merge_strategy_theirs_correctly_marks_key_for_dict_as_dirty():
    # arrange
    target = InProcessParamStore()
    target["foo"] = {"x": {"a": 1}}
    theirs = InProcessParamStore()
    theirs["foo"] = {"x": {"b": 2}}
    target.commit()  # so we start the merge in  a non-dirty state
    # act
    target.merge(theirs, MergeStrategy.THEIRS)
    # assert
    assert target._InProcessParamStore__dirty_keys == {"foo"}
    assert target["foo"]["x"]["a"] == 1 and target["foo"]["x"]["b"] == 2


def test_merge_strategy_theirs_when_ours_is_leaf_theirs_is_dict_then_theirs_is_copied():
    target = InProcessParamStore()
    target["foo"] = "bar"
    theirs = InProcessParamStore()
    theirs["foo"] = {"baz": 1}
    target.merge(theirs, MergeStrategy.THEIRS)
    assert target["foo"] == {"baz": 1}


def test_merge_strategy_theirs_when_ours_is_dict_theirs_is_leaf_then_their_overwrites():
    target = InProcessParamStore()
    target["foo"] = {"bar": 1}
    theirs = InProcessParamStore()
    theirs["foo"] = "baz"
    target.merge(theirs, MergeStrategy.THEIRS)
    assert target["foo"] == "baz"


def test_merge_strategy_theirs_both_sides():
    target = InProcessParamStore()
    target["foo"] = dict(a=1, b=dict(y=5, z=6))
    theirs = InProcessParamStore()
    theirs["foo"] = dict(b=dict(x=4, y=-5), c=3)
    target.merge(theirs, MergeStrategy.THEIRS)
    assert target.to_dict() == {
        "foo": {
            "a": 1,
            "b": {"x": 4, "y": -5, "z": 6},
            "c": 3,
        }
    }


def test_merge_strategy_theirs_when_their_key_is_copied_then_store_is_dirty():
    target = InProcessParamStore()
    theirs = InProcessParamStore()
    theirs["foo"] = "bar"
    target.commit()  # so is_dirty becomes False
    target.merge(theirs, MergeStrategy.THEIRS)
    assert target.is_dirty


""" list_values() """


def test_list_values_when_key_was_never_in_store_then_empty_list_is_returned():
    target = InProcessParamStore()
    assert target.list_values("foo").empty


def test_list_values_when_key_is_dirty_in_store_then_one_value_is_returned():
    target = InProcessParamStore()
    target["foo"] = "bar"
    actual = target.list_values("foo")
    assert len(actual) == 1
    value = actual.iloc[-1]
    assert value["value"] == "bar"
    assert value["time"] is None
    assert value["commit_id"] is None
    assert value["label"] is None


def test_list_values_when_store_is_not_dirty_then_last_value_is_full():
    target = InProcessParamStore()
    target["foo"] = "bar"
    target.commit("label")
    actual = target.list_values("foo")
    last_value = actual.iloc[-1]
    assert last_value["value"] == "bar"
    assert last_value["time"] is not None
    assert last_value["commit_id"] is not None
    assert last_value["label"] == "label"


def test_list_values_when_key_is_dirty_and_in_commit_then_two_values_are_returned():
    target = InProcessParamStore()
    target["foo"] = "bar"
    target.commit()
    target["foo"] = "baz"
    actual = target.list_values("foo")
    assert actual.iloc[0]["value"] == "bar"
    assert actual.iloc[1]["value"] == "baz"


def test_list_values_then_values_are_sorted_by_ns_ascending():
    # arrange
    target = InProcessParamStore()
    target["foo"] = 1
    target.commit()
    target["foo"] = 2
    target.commit("beta")
    target["foo"] = 3
    target.commit("gamma")
    target["foo"] = 4
    # act
    actual = target.list_values("foo")
    # assert
    assert actual.iloc[0]["value"] == 1
    assert actual.iloc[1]["value"] == 2
    assert actual.iloc[2]["value"] == 3
    assert actual.iloc[3]["value"] == 4
    assert actual.iloc[0]["label"] is None
    assert actual.iloc[1]["label"] == "beta"
    assert actual.iloc[2]["label"] == "gamma"
    assert actual.iloc[3]["label"] is None


def test_list_values_then_when_key_is_deleted_it_is_not_in_list_of_values():
    # arrange
    target = InProcessParamStore()
    target["foo"] = 1
    target.commit("alpha")
    del target["foo"]
    target.commit("beta")
    target["foo"] = 3
    target.commit("gamma")
    target["foo"] = 4
    # act
    actual = target.list_values("foo")
    # assert
    assert actual.iloc[0]["value"] == 1
    assert actual.iloc[1]["value"] == 3
    assert actual.iloc[2]["value"] == 4
    assert actual.iloc[0]["label"] == "alpha"
    assert actual.iloc[1]["label"] == "gamma"
    assert actual.iloc[2]["label"] is None


""" Tags """


def test_add_tag_when_key_is_not_in_store_then_keyerror_is_raised():
    target = InProcessParamStore()
    with pytest.raises(KeyError):
        target.add_tag("tag", "foo")


def test_add_tag_when_key_exists_then_tag_is_added():
    target = InProcessParamStore()
    target["foo"] = "bar"
    target.add_tag("tag", "foo")
    assert "foo" in target.list_keys_for_tag("tag")
    assert target.is_dirty


def test_remove_tag_when_key_doesnt_exist_then_nothing_happens():
    target = InProcessParamStore()
    target.commit()  # so is_dirty becomes False
    target.remove_tag("tag", "foo")
    assert not target.is_dirty


def test_remove_tag_when_tag_doesnt_exist_then_nothing_happens():
    target = InProcessParamStore()
    target["foo"] = "bar"
    target.commit()  # so is_dirty becomes False
    target.remove_tag("tag", "foo")
    assert not target.is_dirty


def test_list_keys_for_tag_when_tag_doesnt_exist_then_empty_list_is_returned():
    target = InProcessParamStore()
    assert target.list_keys_for_tag("tag") == []


def test_list_keys_for_tag_when_tag_exists_then_multiple_tags_are_returned():
    target = InProcessParamStore()
    target["foo"] = "bar"
    target["boo"] = "baz"
    target.add_tag("tag", "foo")
    target.add_tag("tag", "boo")
    assert target.list_keys_for_tag("tag") == ["foo", "boo"]


def test_list_tags_for_key_when_key_has_tags_then_they_are_returned():
    target = InProcessParamStore()
    target["foo"] = "bar"
    target.add_tag("tag1", "foo")
    target.add_tag("tag2", "foo")
    assert target.list_tags_for_key("foo") == ["tag1", "tag2"]


def test_list_tags_for_key_when_key_has_no_tags_then_empty_list_is_returned():
    target = InProcessParamStore()
    target["foo"] = "bar"
    assert target.list_tags_for_key("foo") == []


def test_list_tags_for_key_when_key_does_not_exist_then_empty_list_is_returned():
    target = InProcessParamStore()
    assert target.list_tags_for_key("foo") == []


""" temp """


def test_save_temp_and_load_temp(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    target.foo = "bar"
    target.save_temp()
    target.foo = "baz"
    target.load_temp()
    assert target.foo == "bar"


def test_load_temp_when_save_temp_not_called_before_then_error_is_raised(
    tinydb_file_path,
):
    target = InProcessParamStore(tinydb_file_path)
    with pytest.raises(EntropyError):
        target.load_temp()


""" demo test """


def test_demo(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    target["qubit1.flux_capacitor.freq"] = 8.0
    target["qubit1.flux_capacitor.amp"] = 5.0
    target["qubit1.flux_capacitor"] = {"wave": "manifold", "warp": 1337.0}

    target.add_tag("tag1", "qubit1.flux_capacitor.freq")

    print(f"before saving to temp, freq: {target['qubit1.flux_capacitor.freq']}")

    target.save_temp()

    target["qubit1.flux_capacitor.freq"] = 18.0

    print(f"changed after saving, freq: {target['qubit1.flux_capacitor.freq']}")

    target.load_temp()

    print(
        f"loaded previous value from temp, freq: {target['qubit1.flux_capacitor.freq']}"
    )

    commit_id = target.commit("warm-up")

    print(f"first commit freq: {target['qubit1.flux_capacitor.freq']}")

    target["qubit1.flux_capacitor.freq"] = 11.0

    print(f"second commit freq: {target['qubit1.flux_capacitor.freq']}")
    print(
        f"first commit freq from history: "
        f"{target.get_value('qubit1.flux_capacitor.freq', commit_id)}"
    )

    target.commit("warm-up")

    target.checkout(commit_id)

    print(f"checked out freq: {target['qubit1.flux_capacitor.freq']}")

    print("list_commits commits labeled 'warm': ")
    pprint(target.list_commits("warm"))

    print("all params: ")
    pprint(target.to_dict())


def test_migrate_param_store_0_1_to_0_2(tinydb_file_path, request):
    # arrange
    _copy_template("migrate_param_store_0_1_to_0_2.json", tinydb_file_path, request)
    # act
    migrate_param_store_0_1_to_0_2(tinydb_file_path, "test_param_store.py")
    _set_version(tinydb_file_path, "0.2", "test")
    # assert
    param_store = InProcessParamStore(tinydb_file_path)
    # checkout unharmed
    param_store.checkout("57ea4b9fb96bdc7a13fe8ec616a3c6da21f41ca0")
    # primitive value
    assert param_store["qubit1.flux_capacitor.freq"] == 8.0
    # dict value
    assert param_store["qubit1.flux_capacitor"]["wave"] == "manifold"
    # tags are unharmed
    assert "qubit1.flux_capacitor.amp" in param_store.list_keys_for_tag("tag1")
    commit = param_store.list_commits("warm-up")[0]
    assert commit.timestamp == 1652959865653245900
    # temp is unharmed
    param_store.load_temp()
    assert param_store["qubit1.flux_capacitor.freq"] == -8.0


def test_fix_param_qualified_name(tinydb_file_path, request):
    # arrange
    _copy_template("fix_param_qualified_name.json", tinydb_file_path, request)
    # act
    fix_param_qualified_name(tinydb_file_path, "test_param_store.py")
    # assert
    with TinyDB(tinydb_file_path) as tinydb:
        doc = tinydb.get(doc_id=1)
        assert (
            doc["params"]["q0_f_if_01"]["py/object"]
            == "entropylab.pipeline.api.param_store.Param"
        )


""" class Param """


def test_has_expired_when_expiration_is_int_and_has_expired_then_true():
    target = Param(42)
    target.expiration = time.time_ns() - 1
    assert target.has_expired


def test_has_expired_when_expiration_is_int_and_not_expired_then_false():
    target = Param(42)
    target.expiration = time.time_ns() + 1000 * 1e9
    assert not target.has_expired


def test_has_expired_when_expiration_is_none_then_false():
    target = Param(42)
    target.expiration = None
    assert not target.has_expired


def test_has_expired_when_expiration_is_timedelta_then_false():
    target = Param(42)
    target.expiration = timedelta(hours=5)
    assert not target.has_expired


""" Testing multi-process scenarios """


def set_foo_and_commit(path, name: str, num_of_commits: int):
    with InProcessParamStore(path) as target:
        for i in range(num_of_commits):
            target.name = name
            target.date = str(datetime.utcnow())
            target.commit(name)


def test_multi_processes_do_not_conflict(tinydb_file_path):
    # arrange
    num_of_processes = 3
    num_of_commits = 10
    processes = []
    for i in range(num_of_processes):
        processes.append(
            Process(
                target=set_foo_and_commit,
                args=(tinydb_file_path, f"proc{i}", num_of_commits),
            )
        )

    # act
    for p in processes:
        p.start()
    for p in processes:
        p.join()

    # assert no exceptions
    assert all(p.exception is None for p in processes)

    # assert all params committed by all processes
    ps = InProcessParamStore(tinydb_file_path)
    names = ps.list_values("name")["value"]
    assert all(names.value_counts() == num_of_commits)


def test__ns_to_datetime():
    expected = pd.Timestamp(ts_input="2022-07-10 11:40:37.233137200+0300", tz=LOCAL_TZ)
    actual = _ns_to_datetime(1657442437233137200)
    assert actual == expected

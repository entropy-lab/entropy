from pprint import pprint
from time import sleep

import pytest
from tinydb import Query

from entropylab.api.errors import EntropyError
from entropylab.api.in_process_param_store import (
    InProcessParamStore,
    Metadata,
    MergeStrategy,
)

""" ctor """


def test_ctor_is_dirty_is_true():
    target = InProcessParamStore()
    assert target.is_dirty is True


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
        target.commit()
    with open(tinydb_file_path) as f:
        assert "__foo" not in f.read()


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
    actual = target.get("foo")
    # assert
    assert actual == "bar"


def test_get_when_commit_id_is_not_none_then_value_is_returned():
    # arrange
    target = InProcessParamStore()
    target["foo"] = "bar"
    commit_id = target.commit()
    target["foo"] = "baz"
    # act
    actual = target.get("foo", commit_id)
    # assert
    assert actual == "bar"


def test_get_when_commit_id_is_bad_then_entropy_error_is_raised():
    # arrange
    target = InProcessParamStore()
    target["foo"] = "bar"
    # act
    with pytest.raises(EntropyError):
        target.get("foo", "oops")


def test_rename_key_when_key_exists_then_it_is_renamed():
    # arrange
    target = InProcessParamStore()
    target["foo"] = dict(bar="baz")
    # act
    target.rename_key("foo", "new")
    # assert
    assert target["new"]["bar"] == "baz"


def test_rename_key_when_key_does_not_exist_then_an_error_is_raised():
    # arrange
    target = InProcessParamStore()
    # act & assert
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


""" commit() """


def test_commit_in_memory_when_param_changes_commit_doesnt_change():
    # arrange
    target = InProcessParamStore()
    target["foo"] = "bar"
    commit_id = target.commit()
    # act
    target["foo"] = "baz"
    # assert
    assert target.get("foo", commit_id) == "bar"


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
    target = InProcessParamStore(tinydb_file_path)
    commit_id = target.commit()
    result = target._InProcessParamStore__db.search(Query().metadata.id == commit_id)
    assert result[0]["metadata"]["label"] is None


def test_commit_when_label_is_given_then_label_is_saved(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    commit_id = target.commit("foo")
    result = target._InProcessParamStore__db.search(Query().metadata.id == commit_id)
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


def test_checkout_when_commit_id_doesnt_exist_error_is_raised(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    with pytest.raises(EntropyError):
        target.checkout("foo")


def test_checkout_when_commit_num_exists_value_is_reverted(tinydb_file_path):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["foo"] = "bar"
    target.commit()
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
    target.checkout(commit_id)
    assert target.list_keys_for_tag("tag") == []


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


@pytest.mark.parametrize(
    "move_by, expected_val",
    [
        (-1, "foo"),
        (0, "bar"),
        (1, "baz"),
    ],
)
def test_checkout_when_move_by_exists_value_is_reverted(
    tinydb_file_path, move_by, expected_val
):
    # arrange
    target = InProcessParamStore(tinydb_file_path)
    target["val"] = "foo"
    target.commit()
    target["val"] = "bar"
    commit_id = target.commit()
    target["val"] = "baz"
    target.commit()
    target.checkout(commit_id)  # commit_num == 1
    # act
    target.checkout(move_by=move_by)
    # assert
    assert target["val"] == expected_val


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
    assert all(type(m) == Metadata for m in actual)
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
    target.commit("foolabel")
    target["foo"] = "post"
    target.commit("labelfoo")
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
    assert len(actual) == 3


""" _generate_metadata() """


def test__generate_metadata_empty_dict():
    target = InProcessParamStore()
    actual = target._InProcessParamStore__build_metadata()
    assert len(actual.id) == 40


def test__generate_metadata_nonempty_dict():
    target = InProcessParamStore()
    target["foo"] = "bar"
    actual = target._InProcessParamStore__build_metadata()
    assert len(actual.id) == 40


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
    assert actual.iloc[0]["value"] == "bar"
    assert actual.iloc[0]["time"] is None
    assert actual.iloc[0]["commit_id"] is None
    assert actual.iloc[0]["label"] is None


def test_list_values_when_store_is_not_dirty_then_value_is_full():
    target = InProcessParamStore()
    target["foo"] = "bar"
    target.commit("label")
    actual = target.list_values("foo")
    assert actual.iloc[0]["value"] == "bar"
    assert actual.iloc[0]["time"] is not None
    assert actual.iloc[0]["commit_id"] is not None
    assert actual.iloc[0]["label"] == "label"


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
        f"{target.get('qubit1.flux_capacitor.freq', commit_id)}"
    )

    target.commit("warm-up")

    target.checkout(commit_id)

    print(f"checked out freq: {target['qubit1.flux_capacitor.freq']}")

    print("list_commits commits labeled 'warm': ")
    pprint(target.list_commits("warm"))

    print("all params: ")
    pprint(target.to_dict())

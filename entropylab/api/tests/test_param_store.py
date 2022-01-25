from pprint import pprint

import pytest
from tinydb import Query

from entropylab.api.errors import EntropyError
from entropylab.api.param_store import InProcessParamStore, Metadata, MergeStrategy

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


""" log() """


def test_log_no_args_returns_all_metadata(
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
    actual = target.log()
    # assert
    assert all(type(m) == Metadata for m in actual)
    assert actual[0].label == "first"
    assert actual[1].label == "second"
    assert actual[2].label == "third"


def test_log_when_label_exists_then_it_is_returned(
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
    actual = target.log("label")
    # assert
    assert all(type(m) == Metadata for m in actual)
    assert all("label" in m.label for m in actual)
    assert len(actual) == 3


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


def test_merge_strategy_theirs_when_ours_is_dict_theirs_is_leaf_then_theirs_overwrites():
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


""" demo test """


def test_demo(tinydb_file_path):
    target = InProcessParamStore(tinydb_file_path)
    target["qubit1.flux_capacitor.freq"] = 8.0
    target["qubit1.flux_capacitor.amp"] = 5.0
    target["qubit1.flux_capacitor"] = {"wave": "manifold", "warp": 1337.0}

    print(f"before commit freq: {target['qubit1.flux_capacitor.freq']}")

    commit_id = target.commit("warm-up")

    print(f"first commit freq: {target['qubit1.flux_capacitor.freq']}")

    target["qubit1.flux_capacitor.freq"] = 11.0

    print(f"second commit freq: {target['qubit1.flux_capacitor.freq']}")
    print(
        f"first commit freq from history: {target.get('qubit1.flux_capacitor.freq', commit_id)}"
    )

    target.commit("warm-up")

    target.checkout(commit_id)

    print(f"checked out freq: {target['qubit1.flux_capacitor.freq']}")

    print("log commits labeled 'warm': ")
    pprint(target.log("warm"))

    print("all params: ")
    pprint(target.to_dict())

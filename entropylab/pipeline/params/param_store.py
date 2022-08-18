from __future__ import annotations

import copy
import threading
import time
from datetime import timedelta
from enum import unique, Enum
from pathlib import Path
from typing import Optional, Dict, Any, List, Set, MutableMapping

import pandas as pd

from entropylab.config import settings
from entropylab.pipeline.params.persistence.persistence import Commit, Metadata
from entropylab.pipeline.params.persistence.sqlalchemy.sqlalchemypersistence import (
    SqlAlchemyPersistence,
)
from entropylab.pipeline.params.persistence.tinydb.tinydbpersistence import (
    TinyDbPersistence,
)

UTC_TZ = "UTC"


@unique
class MergeStrategy(Enum):
    OURS = 1
    THEIRS = 2


# TODO: Convert to dataclass
class Param(Dict):
    def __init__(self, value):
        super().__init__()
        self.value: object = value
        self.commit_id: Optional[str] = None
        self.expiration: Optional[timedelta | pd.Timestamp] = None
        self.description: Optional[str] = None
        self.node_id: Optional[str] = None

    def __repr__(self):
        return (
            f"<Param(value={self.value}, "
            f"commit_id={self.commit_id}, "
            f"expiration={self.expiration})> "
        )

    @property
    def has_expired(self):
        """
        Indicates whether the Param value has expired. Returns True iff the Param
        has been committed and the time elapsed since the commit operation has exceeded
        the time recorded in the `expiration` property"""
        if isinstance(self.expiration, pd.Timestamp):
            return self.expiration < pd.Timestamp(time.time_ns())
        else:
            return False


class ParamStore(MutableMapping):
    """
    A class that provides versioned storage for experiment parameters (params).
    """

    def __init__(
        self,
        path: Optional[str] | Optional[Path] = None,
        url: Optional[str] | Optional[Path] = None,
        theirs: Optional[Dict | ParamStore] = None,
        merge_strategy: Optional[MergeStrategy] = MergeStrategy.THEIRS,
    ):
        super().__init__()
        self.__lock = threading.RLock()
        self.__params: Dict[str, Param] = dict()  # where current params are stored
        self.__tags: Dict[str, List[str]] = dict()  # tags that are mapped to keys
        self.__dirty_keys: Set[str] = set()  # updated keys not committed yet
        # constructor arguments take precedence:
        if path:
            self.__persistence = TinyDbPersistence(path)
        elif url:
            self.__persistence = SqlAlchemyPersistence(url)
        else:
            # ...over configuration settings
            if "param_store_path" in settings:
                self.__persistence = TinyDbPersistence(settings.param_store_path)
            elif "param_store_url" in settings:
                self.__persistence = SqlAlchemyPersistence(settings.param_store_url)
            else:
                # default
                self.__persistence = TinyDbPersistence()

        self.checkout()
        if theirs is not None:
            self.merge(theirs, merge_strategy)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.__persistence.close()

    """ Properties """

    @property
    def is_dirty(self):
        """
        True iff params have been changed since the store has last been
        initialized or checked out.
        """
        pass
        with self.__lock:
            return len(self.__dirty_keys) > 0

    """ MutableMapping """

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Set self[key] to value. The key-value pair becomes a "param" and
        can be persisted using `commit()` and retrieved later using
        `checkout()`.

        Note: Keys should not start with a dunder (`__`). Such keys are not
        treated as params and are not persisted when `commit()` is called.
        """

        if key.startswith("__") or key.startswith(f"_{self.__class__.__name__}__"):
            # keys that are private attributes are not params and are treated
            # as regular object attributes
            object.__setattr__(self, key, value)
        else:
            with self.__lock:
                self.__params.__setitem__(key, Param(value))
                self.__dirty_keys.add(key)

    def __getitem__(self, key: str) -> Any:
        with self.__lock:
            return self.__params.__getitem__(key).value

    def __delitem__(self, *args, **kwargs):
        with self.__lock:
            key = args[0]
            self.__params.__delitem__(*args, **kwargs)
            self.__remove_key_from_tags(key)
            self.__dirty_keys.add(key)

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            try:
                return self[key]
            except KeyError:
                raise AttributeError(key)

    def __setattr__(self, key, value):
        try:
            object.__getattribute__(self, key)
        except AttributeError:
            try:
                self[key] = value
            except BaseException:
                raise AttributeError(key)
        else:
            object.__setattr__(self, key, value)

    def __iter__(self):
        with self.__lock:
            values = _extract_param_values(self.__params)
            return values.__iter__()

    def __len__(self):
        with self.__lock:
            return self.__params.__len__()

    def __contains__(self, key):
        with self.__lock:
            return self.__params.__contains__(key)

    def __repr__(self):
        with self.__lock:
            return f"<ParamStore({self.to_dict().__repr__()})>"

    """ Params """

    def keys(self):
        with self.__lock:
            return self.__params.keys()

    def to_dict(self) -> Dict:
        with self.__lock:
            return _extract_param_values(self.__params)

    def get_value(self, key: str, commit_id: Optional[str] = None) -> object:
        """
        Returns the value of a param by its key

        :param key: the key identifying the param
        :param commit_id: an optional commit_id. If provided, the value will be
        returned from the specified commit
        """
        with self.__lock:
            if commit_id is None:
                return self[key]
            else:
                commit = self.__persistence.get_commit(commit_id)
                return commit.params[key].value

    def get_param(self, key: str, commit_id: Optional[str] = None) -> Param:
        """
        Returns a copy of the Param instance of a value stored in ParamStore

        :param key: the key identifying the param
        :param commit_id: an optional commit_id. If provided, the Param will be
        returned from the specified commit
        """
        with self.__lock:
            if commit_id is None:
                return copy.deepcopy(self.__params[key])
            else:
                commit = self.__persistence.get_commit(commit_id)
                return copy.deepcopy(commit.params[key])

    def set_param(self, key: str, value: object, **kwargs):
        """
        Sets a Param in the ParamStore

        :param key: the key identifying the param
        :param value: the value of the param
        :param key: an optional period of time (measured from the time the param
        is committed) after which the prime expires (i.e. param.has_expired
        returns True)
        """
        if "commit_id" in kwargs:
            raise ValueError("Setting commit_id in set_param() is not allowed")
        if "value" in kwargs:
            raise ValueError("Value can only be set through positional argument")
        with self.__lock:
            if key in self.__params:
                param = self.get_param(key)
            else:
                param = Param(value)
            param.value = value
            param.__dict__.update(kwargs)
            self.__params.__setitem__(key, param)
            self.__dirty_keys.add(key)

    def __remove_key_from_tags(self, key: str):
        for tag in self.__tags:
            if key in self.__tags[tag]:
                self.__tags[tag].remove(key)

    def rename_key(self, key: str, new_key: str):
        with self.__lock:
            if new_key in self.keys():
                raise KeyError(
                    f"Cannot rename key '{key}' to key that already exists: '{new_key}'"
                )
            self.__rename_key_in_tags(key, new_key)
            value = self.__getitem__(key)
            self.__setitem__(new_key, value)
            self.__delitem__(key)

    def __rename_key_in_tags(self, key, new_key):
        for item in self.__tags.items():
            tag = item[0]
            keys = item[1]
            if key in keys:
                self.__tags[tag].remove(key)
                self.__tags[tag].append(new_key)

    """ Commits """

    def commit(self, label: Optional[str] = None) -> str:
        with self.__lock:
            commit = Commit(
                label=label,
                params=self.__params,
                tags=self.__tags,
            )
            commit_id = self.__persistence.commit(commit, self.__dirty_keys)
            self.__dirty_keys.clear()
            return commit_id

    def checkout(
        self, commit_id: Optional[str] = None, commit_num: Optional[int] = None
    ) -> None:
        with self.__lock:
            commit = self.__persistence.get_commit(commit_id, commit_num)
            if commit:
                self.__checkout(commit)

    def __checkout(self, commit: Commit):
        self.__params.clear()
        self.__params.update(commit.params)
        self.__tags.clear()
        self.__tags.update(commit.tags)
        self.__dirty_keys.clear()

    def list_commits(self, label: Optional[str] = None) -> List[Metadata]:
        """
        Returns a list of commits

        :param label: an optional label, if given then only commits that match
        it will be returned
        """
        with self.__lock:
            commits = self.__persistence.search_commits(label)
            metadata = map(Commit.to_metadata, commits)
            return list(metadata)

    """ Merge """

    def merge(
        self,
        theirs: ParamStore,
        merge_strategy: Optional[MergeStrategy] = MergeStrategy.OURS,
    ) -> None:
        with self.__lock:
            ours = self
            self.__merge_trees(ours, theirs, merge_strategy)

    def __merge_trees(
        self,
        a: ParamStore | Dict,
        b: ParamStore | Dict,
        merge_strategy: MergeStrategy,
    ) -> bool:
        """Merges `b` into `a` *in-place* using the given strategy"""
        a_has_changed = False
        for key in b.keys():
            if key in a.keys():
                if (
                    (not isinstance(a[key], Param))
                    and isinstance(a[key], dict)
                    and (not isinstance(b[key], Param))
                    and isinstance(b[key], dict)
                ):
                    """This is a special case where the values of the Params are both
                    dictionaries. In this case we merge the dictionary from b into the
                    dictionary from a using the given strategy."""
                    a_has_changed = a_has_changed or self.__merge_trees(
                        a[key], b[key], merge_strategy
                    )
                    if (
                        a_has_changed
                        and isinstance(a, ParamStore)
                        and isinstance(b, ParamStore)
                    ):
                        """if the dictionary in a has been changed, this was done
                        in-place. We therefore need to mark the Param key as dirty. We
                        only do this at the very top of the recursion - when a and b
                        are the ParamStores being merged"""
                        self.__dirty_keys.add(key)
                elif a[key] == b[key]:
                    pass  # same leaf values, nothing to do
                else:  # diff leave values => conflict:
                    if merge_strategy == MergeStrategy.OURS:
                        pass  # a takes precedence, ignore b
                    elif merge_strategy == MergeStrategy.THEIRS:
                        a[key] = b[key]  # b takes precedence, overwrite a
                        a_has_changed = True
                    else:
                        raise NotImplementedError(
                            f"MergeStrategy '{merge_strategy}' is not implemented"
                        )
            else:  # key from b is not in a:
                a[key] = b[key]  # "copy" from b to a
                a_has_changed = True
        return a_has_changed

    """ Diff """

    def diff(
        self, old_commit_id: Optional[str] = None, new_commit_id: Optional[str] = None
    ) -> Dict[str, Dict]:
        """Shows the difference in Param values between two commits.

        :param old_commit_id: The id of the first ("older") commit to compare. If
            None, or not specified, defaults to the latest commit id.
        :param new_commit_id: The id of the second  ("newer") commit to compare. If
            None, or not specified, defaults to the current state of the store (incl.
             "dirty" values)
        :return: A dictionary where keys are the keys of params whose values have
            changed. Dictionary values indicate the `old_value` of the param and the
            `new_value` of the param. A new param will only show the `new_value`. A
            deleted param will only show the `old_value`.
            Example: {"foo": {"old_value": "bar", "new_value": "baz"}}
        """
        with self.__lock:

            # get OLD params to diff:
            if old_commit_id:
                old_commit = self.__persistence.get_commit(old_commit_id)
            else:  # default to latest commit
                old_commit = self.__persistence.get_latest_commit()
            old_params = old_commit.params if old_commit else {}

            # get NEW params to diff:
            if new_commit_id:
                new_commit = self.__persistence.get_commit(new_commit_id)
                new_params = new_commit.params if new_commit else {}
            else:  # default to dirty params
                new_params = self.__params

            return self.__diff(old_params, new_params)

    @staticmethod
    def __diff(old: MutableMapping, new: MutableMapping) -> Dict[str, Dict]:
        diff = dict()
        for key in new.keys():
            if key in old.keys():
                old_value = old[key].value
                new_value = new[key].value
                if old_value != new_value:  # different values
                    diff[key] = dict(old_value=old_value, new_value=new_value)
            else:
                diff[key] = dict(new_value=new[key].value)  # added
        for key in old.keys():
            if key not in new.keys():
                diff[key] = dict(old_value=old[key].value)  # deleted
        return diff

    @staticmethod
    def __safe_get_value_from_params(params: Dict[str, Param], key: str) -> Optional:
        if key in params:
            return params[key].value
        else:
            return None

    def list_values(self, key: str) -> pd.DataFrame:
        """
        Lists all the values of a given key taken from commit history,
        sorted by date ascending

        :param key: the key for which to list values
        :returns: a list of tuples where the values are, in order:
            - the value of the key
            - time of commit
            - commit_id
            - label assigned to commit
        """
        with self.__lock:
            values = []
            commits = self.__persistence.search_commits(key=key)
            commits.sort(key=lambda c: c.timestamp)
            for commit in commits:
                value = (
                    commit.params[key].value,
                    commit.local_timestamp,
                    commit.id,
                    commit.label,
                )
                values.append(value)
            if self.is_dirty and key in self.__params.keys():
                values.append((self[key], None, None, None))
            df = pd.DataFrame(values)
            if not df.empty:
                df.columns = ["value", "time", "commit_id", "label"]
            return df

    """ Tags """

    def add_tag(self, tag: str, key: str) -> None:
        with self.__lock:
            if key not in self.__params.keys():
                raise KeyError(f"key '{key}' is not in store")
            if tag not in self.__tags:
                self.__tags[tag] = []
            self.__tags[tag].append(key)

    def remove_tag(self, tag: str, key: str) -> None:
        with self.__lock:
            if tag not in self.__tags:
                return
            if key not in self.__tags[tag]:
                return
            self.__tags[tag].remove(key)

    def list_keys_for_tag(self, tag: str) -> List[str]:
        with self.__lock:
            if tag not in self.__tags:
                return []
            else:
                return self.__tags[tag]

    def list_tags_for_key(self, key: str):
        tags_for_key = []
        for item in self.__tags.items():
            if key in item[1]:
                tags_for_key.append(item[0])
        return tags_for_key

    """ Temporary State """

    def save_temp(self) -> None:
        """
        Saves the state of params to a temporary location
        """
        with self.__lock:
            commit = Commit(
                params=self.__params,
                tags=self.__tags,
            )
            self.__persistence.save_temp_commit(commit)

    def load_temp(self) -> None:
        """
        Overwrites the current state of params with data loaded from the temporary
        location
        """
        with self.__lock:
            commit = self.__persistence.load_temp_commit()
            self.__params.clear()
            self.__params.update(commit.params)
            self.__tags.clear()
            self.__tags.update(commit.tags)


""" Static helper methods """


def _map_dict(f, d: Dict) -> Dict:
    """Applies function f to all values in dict d recursively"""
    values_dict = dict()
    for item in d.items():
        k = item[0]
        v = item[1]
        if not isinstance(v, Param) and isinstance(v, dict):
            values_dict[k] = _map_dict(f, v)
        else:
            values_dict[k] = f(v)
    return values_dict


def _extract_param_values(d: Dict) -> Dict:
    return _map_dict(lambda x: x.value, d)

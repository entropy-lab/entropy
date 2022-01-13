import hashlib
import json
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum, unique
from typing import Dict, List, Any

from tinydb import TinyDB


@unique
class MergeStrategy(Enum):
    OURS = (1,)
    THEIRS = (2,)
    RECURSIVE = 3


class Commit:
    id: str
    datetime: datetime


class ParamStore(ABC):
    def __init__(self):
        super().__init__()

    """ Present dictionary """

    @abstractmethod
    def __getitem__(self, key: str) -> Any:
        pass

    @abstractmethod
    def __setitem__(self, key: str, value: Any) -> None:
        pass

    @abstractmethod
    def commit(self) -> str:
        pass

    @abstractmethod
    def to_dict(self) -> Dict:
        pass

    # def merge(
    #     self,
    #     theirs: ParamStore,
    #     merge_strategy: Optional[MergeStrategy] = MergeStrategy.OURS,
    # ) -> None:
    #     pass

    def search_for_label(self, label: str) -> List[Commit]:
        pass


class InProcessParamStore(ParamStore):
    def __init__(self):
        super().__init__()
        self._dict = dict()
        self._db = TinyDB()

    """ Present dictionary """

    def __setitem__(self, key: str, value: Any) -> None:
        self._dict[key] = value

    def __getitem__(self, key: str) -> Any:
        return self._dict[key]

    def to_dict(self) -> Dict:
        return dict(self._dict)

    """ Commits """

    def commit(self) -> str:
        commit_id = self._generate_id()
        self._db.insert({"_id": commit_id} | self._dict)
        return commit_id

    def _generate_id(self):
        dict_as_string = json.dumps(
            self._dict, sort_keys=True, ensure_ascii=True
        ).encode("utf-8")
        return hashlib.sha1(dict_as_string).hexdigest()

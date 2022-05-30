import os.path
import pickle
from datetime import datetime
from enum import Enum
from typing import Optional, Any, Iterable, TypeVar, Callable, List

import h5py
import numpy as np

from entropylab import RawResultData
from entropylab.pipeline.api.data_reader import ResultRecord, MetadataRecord
from entropylab.pipeline.api.data_writer import Metadata
from entropylab.logger import logger
from entropylab.pipeline.results_backend.sqlalchemy.model import (
    ResultDataType,
    ResultTable,
    Base,
)

T = TypeVar("T", bound=Base)
R = TypeVar("R", ResultRecord, MetadataRecord)


def _experiment_from(dset: h5py.Dataset) -> int:
    return dset.attrs["experiment_id"]


def _id_from(dset: h5py.Dataset) -> str:
    return dset.name


def _stage_from(dset: h5py.Dataset) -> int:
    return dset.attrs["stage"]


def _label_from(dset: h5py.Dataset) -> str:
    return dset.attrs["label"]


def _story_from(dset: h5py.Dataset) -> str:
    if "story" in dset.attrs:
        return dset.attrs["story"]
    return ""


def _data_from(dset: h5py.Dataset) -> Any:
    data = dset[()]
    if dset.dtype.metadata and dset.dtype.metadata.get("vlen") == str:
        return dset.asstr()[()]
    elif dset.attrs.get("data_type") == ResultDataType.Pickled.value:
        return pickle.loads(data)
    elif dset.attrs.get("data_type") == ResultDataType.String.value:
        # un-picklable data is stored as HDF5 Opaque so turn to bytes then to string:
        return data.tobytes().decode(encoding="utf-8")
    else:
        return data


def _time_from(dset: h5py.Dataset) -> datetime:
    return datetime.fromisoformat(dset.attrs["time"])


def _build_result_record(dset: h5py.Dataset) -> ResultRecord:
    return ResultRecord(
        experiment_id=_experiment_from(dset),
        id=_id_from(dset),
        label=_label_from(dset),
        story=_story_from(dset),
        stage=_stage_from(dset),
        data=_data_from(dset),
        time=_time_from(dset),
    )


def _build_metadata_record(dset: h5py.Dataset) -> MetadataRecord:
    return MetadataRecord(
        experiment_id=_experiment_from(dset),
        id=_id_from(dset),
        label=_label_from(dset),
        stage=_stage_from(dset),
        data=_data_from(dset),
        time=_time_from(dset),
    )


def _get_all_or_single(group: h5py.Group, name: Optional[str] = None):
    """
    Returns all or one child from an h5py.Group

    Parameters
    ----------
    group group to get child or children from. Can be h5py.File itself.
    name name of child to get. If None, indicates all children should be retrieved.

    Returns
    -------
    A list of group children (either h5py.Group or h5py.Datasets)
    """
    if name is None:
        return list(group.values())
    else:
        if str(name) in group:
            return [group[str(name)]]
        else:
            return []


class EntityType(Enum):
    RESULT = 1
    METADATA = 2


class _HDF5Reader:
    def get_result_records(
        self,
        experiment_id: Optional[int] = None,
        stage: Optional[int] = None,
        label: Optional[str] = None,
    ) -> Iterable[ResultRecord]:
        return self._get_records(
            EntityType.RESULT, _build_result_record, experiment_id, stage, label
        )

    def get_metadata_records(
        self,
        experiment_id: Optional[int] = None,
        stage: Optional[int] = None,
        label: Optional[str] = None,
    ) -> Iterable[MetadataRecord]:
        return self._get_records(
            EntityType.METADATA, _build_metadata_record, experiment_id, stage, label
        )

    def _get_records(
        self,
        entity_type: EntityType,
        record_build_func: Callable,
        experiment_id: Optional[int] = None,
        stage: Optional[int] = None,
        label: Optional[str] = None,
    ) -> Iterable[T]:
        entities = []
        if experiment_id:
            experiment_ids = [experiment_id]
        else:
            experiment_ids = self._list_experiment_ids_in_fs()
        for experiment_id in experiment_ids:
            entities += self._get_experiment_entities(
                entity_type, record_build_func, experiment_id, stage, label
            )
        return sorted(entities, key=lambda entity: entity.experiment_id)

    def _list_experiment_ids_in_fs(self) -> List[int]:
        # noinspection PyUnresolvedReferences
        dir_list = os.listdir(self._path)
        # TODO: Better validation of experiment ids
        exp_files = filter(lambda f: f.endswith(".hdf5"), dir_list)
        experiment_ids = list(map(lambda f: f[:-5], exp_files))
        return experiment_ids

    def _get_experiment_entities(
        self,
        entity_type: EntityType,
        convert_from_dset: Callable,
        experiment_id: int,
        stage: Optional[int] = None,
        label: Optional[str] = None,
    ) -> Iterable[T]:
        dsets = []
        try:
            # noinspection PyUnresolvedReferences
            file = self._open_hdf5(experiment_id, "r")
        except FileNotFoundError:
            logger.error(f"HDF5 file for experiment_id [{experiment_id}] was not found")
        else:
            with file:
                stage_groups = _get_all_or_single(file, stage)
                for stage_group in stage_groups:
                    label_groups = _get_all_or_single(stage_group, label)
                    for label_group in label_groups:
                        dset_name = entity_type.name.lower()
                        dset = label_group[dset_name]
                        dsets.append(convert_from_dset(dset))
        return dsets

    def get_last_result_of_experiment(
        self, experiment_id: int
    ) -> Optional[ResultRecord]:
        results = list(self.get_result_records(experiment_id, None, None))
        if results and len(results) > 0:
            results.sort(key=lambda x: x.time, reverse=True)
            return results[0]
        else:
            return None


class _HDF5Writer:
    def save_result(self, experiment_id: int, result: RawResultData) -> str:
        # noinspection PyUnresolvedReferences
        with self._open_hdf5(experiment_id, "a") as file:
            return self._save_entity_to_file(
                file,
                EntityType.RESULT,
                experiment_id,
                result.stage,
                result.label,
                result.data,
                datetime.now(),
                result.story,
            )

    def save_metadata(self, experiment_id: int, metadata: Metadata):
        # noinspection PyUnresolvedReferences
        with self._open_hdf5(experiment_id, "a") as file:
            return self._save_entity_to_file(
                file,
                EntityType.METADATA,
                experiment_id,
                metadata.stage,
                metadata.label,
                metadata.data,
                datetime.now(),
            )

    def _save_entity_to_file(
        self,
        file: h5py.File,
        entity_type: EntityType,
        experiment_id: int,
        stage: int,
        label: str,
        data: Any,
        time: datetime,
        story: Optional[str] = None,
        migrated_id: Optional[str] = None,
    ) -> str:
        path = f"/{stage}/{label}"
        label_group = file.require_group(path)
        dset = self._create_dataset(label_group, entity_type, data)
        dset.attrs.create("experiment_id", experiment_id)
        dset.attrs.create("stage", stage)
        dset.attrs.create("label", label)
        dset.attrs.create("time", time.astimezone().isoformat())
        if story:
            dset.attrs.create("story", story or "")
        if migrated_id:
            dset.attrs.create("migrated_id", migrated_id or "")
        return dset.name

    def _create_dataset(
        self, group: h5py.Group, entity_type: EntityType, data: Any
    ) -> h5py.Dataset:
        name = entity_type.name.lower()
        try:
            dset = group.create_dataset(name=name, data=data)
        except TypeError:
            data_type, pickled = self._pickle_data(data)
            # np.void turns our string to bytes (HDF5 Opaque):
            dset = group.create_dataset(name=name, data=np.void(pickled))
            dset.attrs.create("data_type", data_type.value, dtype="i2")
        return dset

    @staticmethod
    def _pickle_data(data: Any) -> (bytes, ResultDataType):
        try:
            pickled = pickle.dumps(data)
            data_type = ResultDataType.Pickled
        except Exception as ex:
            logger.debug("Could not pickle data, defaulting to __repr__()", ex)
            pickled = data.__repr__().encode(encoding="utf-8")
            data_type = ResultDataType.String
        return data_type, pickled


class _HDF5Migrator(_HDF5Writer):
    def migrate_result_rows(self, rows: Iterable[ResultTable]) -> None:
        self.migrate_rows(EntityType.RESULT, rows)

    def migrate_metadata_rows(self, rows: Iterable[Metadata]) -> None:
        self.migrate_rows(EntityType.METADATA, rows)

    def migrate_rows(self, entity_type: EntityType, rows: Iterable[T]) -> None:
        if rows is not None and len(list(rows)) > 0:
            for row in rows:
                if not row.saved_in_hdf5:
                    record = row.to_record()
                    # noinspection PyUnresolvedReferences
                    with self._open_hdf5(record.experiment_id, "a") as file:
                        hdf5_id = self._migrate_record(file, entity_type, record)
                    logger.debug(
                        f"Migrated ${entity_type.name} with id [{row.id}] "
                        f"to HDF5 with id [{hdf5_id}]"
                    )

    def _migrate_record(
        self, file: h5py.File, entity_type: EntityType, record: R
    ) -> str:
        return self._save_entity_to_file(
            file,
            entity_type,
            record.experiment_id,
            record.stage,
            record.label,
            record.data,
            record.time,
            getattr(record, "story", None),
            record.id,
        )

    def _migrate_result_record(
        self, file: h5py.File, result_record: ResultRecord
    ) -> str:
        return self._save_entity_to_file(
            file,
            EntityType.RESULT,
            result_record.experiment_id,
            result_record.stage,
            result_record.label,
            result_record.data,
            result_record.time,
            result_record.story,
            result_record.id,
        )

    def migrate_from_per_project_hdf5_to_per_experiment_hdf5_files(
        self, old_global_hdf5_file_path
    ):
        logger.debug(
            f"Migrating global .hdf5 file {old_global_hdf5_file_path} "
            "to per-experiment .hdf5 files"
        )
        with h5py.File(old_global_hdf5_file_path, "r") as file:
            if "experiments" in file:
                top_group = file["experiments"]
                for exp_group in top_group.values():
                    experiment_id = int(exp_group.name[13:])
                    f"Migrating results and metadata for experiment id {experiment_id}"
                    # noinspection PyUnresolvedReferences
                    with self._open_hdf5(experiment_id, "a") as exp_file:
                        for stage_group in exp_group.values():
                            exp_group.copy(stage_group, exp_file)
        new_filename = f"{old_global_hdf5_file_path}.bak"
        logger.debug(f"Renaming global .hdf5 file to [{new_filename}]")
        os.rename(old_global_hdf5_file_path, new_filename)
        logger.debug("Global .hdf5 file migration done")


class HDF5Storage(_HDF5Reader, _HDF5Migrator, _HDF5Writer):
    def __init__(self, path=None):
        """Initializes a new storage class instance  for storing experiment results
                 and metadata in HDF5 files.

        :param path: filesystem path to a directory where HDF5 files reside. If no path
                 is given or the path is empty, HDF5 files are stored in memory only.
        """
        if path is None or path == "":  # memory files
            self._path = "./entropy_temp_hdf5"
            self._in_memory_mode = True
        else:  # filesystem
            self._path = path
            os.makedirs(self._path, exist_ok=True)
            self._in_memory_mode = False

    def _open_hdf5(self, experiment_id: int, mode: str) -> h5py.File:
        path = self._build_hdf5_filepath(experiment_id)
        try:
            if self._in_memory_mode:
                """Note that because backing_store=False, self._path is ignored & no
                file is saved on disk.
                See https://docs.h5py.org/en/stable/high/file.html#file-drivers
                """
                return h5py.File(path, mode, driver="core", backing_store=False)
            else:
                return h5py.File(path, mode)
        except FileNotFoundError:
            logger.exception(f"HDF5 file not found at '{path}'")
            raise

    def _build_hdf5_filepath(self, experiment_id: int) -> str:
        return os.path.join(self._path, f"{experiment_id}.hdf5")

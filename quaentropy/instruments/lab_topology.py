import abc
import enum
import inspect
from dataclasses import dataclass
from typing import Type, Optional, Dict, Any, Iterable, List

import jsonpickle

from quaentropy.api.data_reader import DataReader
from quaentropy.api.data_writer import DataWriter
from quaentropy.api.errors import ResourceNotFound
from quaentropy.instruments.instrument_driver import Resource, Instrument
from quaentropy.logger import logger


class DriverType(enum.Enum):
    Packaged = 1
    Local = 2


@dataclass
class ResourceRecord:
    name: str
    class_name: str
    driver_code: str
    version: str
    driver_type: DriverType
    args: str


class PersistentLabDB(abc.ABC):
    def __init__(self) -> None:
        super().__init__()

    @abc.abstractmethod
    def save_new_resource_driver(
        self, name: str, driver_source_code: str, type_name: str, serialized_args: str
    ):
        pass

    @abc.abstractmethod
    def save_state(self, name: str, state: str, snapshot_name: str):
        pass

    @abc.abstractmethod
    def get_state(self, resource_name: str, snapshot_name: str) -> str:
        pass

    @abc.abstractmethod
    def get_all_states(self, name) -> Iterable[str]:
        pass

    @abc.abstractmethod
    def get_resource(self, name) -> Optional[ResourceRecord]:
        pass

    @abc.abstractmethod
    def set_locked(self, resource_name):
        pass

    @abc.abstractmethod
    def set_released(self, resource_name):
        pass


@dataclass
class _LabTopologyResourceInstance:
    name: str
    resource_class: Optional[Type]
    args: Optional[Any]
    instance: Optional[Any]


class PersistentLab:
    def __init__(self, persistence_db: PersistentLabDB) -> None:
        super().__init__()
        self._resources: Dict[str, _LabTopologyResourceInstance] = {}
        self._persistent_db = persistence_db

    def resource_exist(self, name: str, resource_class: Type) -> bool:
        if name in self._resources:
            return True
        else:
            return self._get_resource_record(name, resource_class) is not None

    def _get_resource_record(
        self, name: str, resource_class: Type
    ) -> Optional[ResourceRecord]:
        resource_record = self._persistent_db.get_resource(name)
        if resource_record:
            # TODO: check if same version and code as given resource
            return resource_record
        else:
            return None

    def get_resource(
        self, name: str, resource_class: Type, runtime_args: Optional[List] = None
    ):
        if name not in self._resources:
            resource_instance = self._get_instance(
                name, resource_class, runtime_args=runtime_args
            )
            self._resources[name] = resource_instance
            return resource_instance.instance
        else:
            return self._resources[name].instance

    def _get_resource_if_already_initialized(self, name: str):
        if name in self._resources:
            return self._resources[name].instance
        else:
            raise ResourceNotFound()

    def _get_instance(
        self, name, resource_class: Type, runtime_args: Optional[List] = None
    ):
        if name in self._resources:
            logger.debug(f"got device {name} from memory")
            return self._resources[name]
        else:
            record = self._get_resource_record(name, resource_class)
            if record:
                args = jsonpickle.loads(record.args)
                if runtime_args:
                    combined_args = list(*args) + list(*runtime_args)
                else:
                    combined_args = args
                instance = resource_class(*combined_args)
                return _LabTopologyResourceInstance(
                    record.name, resource_class, record.args, instance
                )

        raise ResourceNotFound()

    def register_resource(
        self, name, resource_class: Type, *args, runtime_args: Optional[List] = None
    ):
        if name in self._resources or self.resource_exist(name, resource_class):
            raise KeyError(f"instrument {name} already exist")
        is_entropy_resource = issubclass(resource_class, Resource)
        if not is_entropy_resource:
            raise TypeError(
                f"instrument {name} is not an quaentropy Resource and"
                f" additional metadata won't be saved"
            )
        logger.debug(f"Initialize device {name}")
        if runtime_args:
            combined_args = list(*args) + list(*runtime_args)
        else:
            combined_args = args
        instance = resource_class(*combined_args)
        self._resources[name] = _LabTopologyResourceInstance(
            name, resource_class, combined_args, instance
        )
        serialized_args = jsonpickle.dumps(list(args))
        self._persistent_db.save_new_resource_driver(
            name,
            inspect.getsource(inspect.getmodule(resource_class)),
            resource_class.__module__ + "." + resource_class.__name__,
            serialized_args,
        )

    def register_resource_if_not_exist(
        self, name, resource_class: Type, *args, runtime_args: Optional[List] = None
    ):
        if name not in self._resources and not self.resource_exist(
            name, resource_class
        ):
            self.register_resource(
                name, resource_class, *args, runtime_args=runtime_args
            )

    def save_snapshot(self, resource_name: str, snapshot_name: str):
        if resource_name not in self._resources:
            raise ResourceNotFound()

        device = self._resources[resource_name]
        is_entropy_resource = issubclass(device.resource_class, Resource)
        if is_entropy_resource:
            str_snapshot = device.instance.snapshot(False)
        else:
            raise TypeError(
                "resource is not an entropy resource and snapshot can't be saved"
            )
        self._persistent_db.save_state(device.name, str_snapshot, snapshot_name)

    def list_resources(self) -> Dict[str, Resource]:
        return {
            resource.name: resource.instance for resource in self._resources.values()
        }

    def lock_resources(self, resources_names: List[str]):
        for resource_name in resources_names:
            resource_record = self._persistent_db.get_resource(resource_name)
            if resource_record:
                self._persistent_db.set_locked(resource_name)
            else:
                raise ResourceNotFound()

    def release_resources(self, resources_names: List[str]):
        failed = False
        for resource_name in resources_names:
            try:
                if resource_name in self._resources:
                    resource = self._resources[resource_name]
                    if isinstance(resource, Instrument):
                        resource.teardown_driver()
                    self._persistent_db.set_released(resource_name)
            except Exception:
                failed = True

        if failed:
            raise Exception("failed releasing some resource")

    def get_resource_info(self, resource_name) -> Dict:
        resource_record = self._persistent_db.get_resource(resource_name)
        if resource_record:
            return {"resource_class": resource_record.class_name}
        else:
            raise ResourceNotFound()

    def get_snapshot(self, resource_name: str, snapshot_name: str) -> str:
        return self._persistent_db.get_state(resource_name, snapshot_name)

    def update_resource_version(self, resource_name, resource_class):
        raise NotImplementedError()  # TODO

    def remove_resource(self, resource_name):
        raise NotImplementedError()  # TODO


class ExperimentResources:
    def __init__(self, persistent_db: Optional[PersistentLabDB] = None) -> None:
        super().__init__()
        self._save_to_db = True
        self._results_db = None
        if isinstance(persistent_db, DataWriter):
            self._results_db = persistent_db
        if isinstance(persistent_db, PersistentLabDB):
            self._persistent_lab_connector = PersistentLab(persistent_db)
        self._private_results_db = None
        self._resources: Dict[str, Type] = {}
        self._local_resources: Dict[str, Any] = {}

    def _get_persistent_lab_connector(self):
        if not self._persistent_lab_connector:
            raise Exception("persistent lab is not configured for this experiment")
        return self._persistent_lab_connector

    def register_private_results_db(self, db):
        self._private_results_db = db

    def pause_save_to_results_db(self):
        logger.warn(
            "DB saving paused, results will be permanently lost on session close"
        )
        self._save_to_db = False

    def resume_save_to_results_db(self):
        logger.warn("DB saving resumed, results will be safely saved to db")
        self._save_to_db = True

    def get_results_db(self) -> Optional:
        if not self._save_to_db:
            return None
        if self._private_results_db:
            return self._private_results_db
        return self._results_db

    def get_results_reader(self) -> DataReader:
        return self.get_results_db()

    def import_persistent_resource(
        self,
        name: str,
        resource_class: Type,
        runtime_args: Optional[List] = None,
        snapshot_name: Optional[str] = None,
    ):
        if (
            self._get_persistent_lab_connector().resource_exist(name, resource_class)
            and name not in self._resources
        ):
            self._resources[name] = resource_class
            self._get_persistent_lab_connector().get_resource(
                name, self._resources[name], runtime_args=runtime_args
            )
            if snapshot_name:
                if issubclass(resource_class, Resource):
                    snap = self._get_persistent_lab_connector().get_snapshot(
                        name, snapshot_name
                    )
                    resource = self.get_resource(name)
                    resource.revert_to_snapshot(snap)
                else:
                    raise Exception(
                        "Resource is not an entropy resource and can't be reverted"
                    )

    def add_temp_resource(self, name: str, instance):
        if name not in self._local_resources:
            self._local_resources[name] = instance
        else:
            raise Exception(f"resource {name} already exist")

    def get_resource(self, name):
        if name not in self._resources and name not in self._local_resources:
            raise Exception(
                "can not used resource that wasn't added to experiment resources"
            )
        if name in self._resources:
            return self._get_persistent_lab_connector()._get_resource_if_already_initialized(
                name
            )
        if name in self._local_resources:
            return self._local_resources[name]

    def lock_all_resources(self):
        if self._resources:
            self._get_persistent_lab_connector().lock_resources(
                [resource_name for resource_name in self._resources]
            )

    def release_all_resources(self):
        if self._resources:
            self._get_persistent_lab_connector().release_resources(
                [resource_name for resource_name in self._resources]
            )
        if self._local_resources:
            for resource_name in self._local_resources:
                resource = self._local_resources[resource_name]
                if isinstance(resource, Instrument):
                    resource.teardown_driver()
                del resource

    def serialize_resources_snapshot(self) -> Dict[str, str]:
        snapshots = {}
        for resource_name in self._resources:
            lab = self._get_persistent_lab_connector()
            resource = lab._get_resource_if_already_initialized(resource_name)
            if resource and isinstance(resource, Resource):
                snapshots[resource_name] = resource.snapshot(False)
        return snapshots

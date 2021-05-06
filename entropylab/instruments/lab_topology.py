import abc
import enum
import importlib
import inspect
from dataclasses import dataclass
from typing import Type, Optional, Dict, Any, Iterable, List, Set

import jsonpickle

from entropylab.api.data_reader import DataReader
from entropylab.api.data_writer import DataWriter
from entropylab.api.errors import ResourceNotFound, EntropyError
from entropylab.instruments.instrument_driver import Resource, Instrument
from entropylab.logger import logger


class DriverType(enum.Enum):
    Packaged = 1
    Local = 2


@dataclass
class ResourceRecord:
    name: str
    module: str
    class_name: str
    driver_code: str
    version: str
    driver_type: DriverType
    args: str
    kwargs: str


class PersistentLabDB(abc.ABC):
    """
    An abstract class for Entropy database, defines the way entropy saved lab data
    A subclass of this class will enable saving and sharing the lab between
    different executions and experiments
    """

    def __init__(self) -> None:
        super().__init__()

    @abc.abstractmethod
    def save_new_resource_driver(
        self,
        name: str,
        driver_source_code: str,
        module: str,
        class_name: str,
        serialized_args: str,
        serialized_kwargs: str,
        number_of_experiment_args,
        keys_of_experiment_kwargs,
    ):
        """
            saves all required data for restoring a resource
        :param name: resource name, should be unique
        :param driver_source_code: driver source code for debugging
        :param module: driver python module
        :param class_name: driver python class
        :param serialized_args: arguments for initializing the driver
        :param serialized_kwargs: key word arguments for initializing the driver
        :param number_of_experiment_args: the number of arguments that are passed
                                            on every request
        :param keys_of_experiment_kwargs: the keys of key word arguments that are
                                        passed on every request
        """
        pass

    @abc.abstractmethod
    def remove_resource(
        self,
        name: str,
    ):
        """
            remove a resource from the persistent lab
        :param name: resource name
        """
        pass

    @abc.abstractmethod
    def save_state(self, name: str, state: str, snapshot_name: str):
        """
            save a specific state/snapshot of the given resource
            If resource implemented the revert_to_snapshot function,
            it can be used for restoring this state to the resource.
        :param name: resource name
        :param state: serialized state
        :param snapshot_name: the name of the snapshot for future use.
                            should be unique for a given resource.
        """
        pass

    @abc.abstractmethod
    def get_state(self, resource_name: str, snapshot_name: str) -> str:
        """
            return resource serialized state
        :param resource_name: the name of the resource
        :param snapshot_name: the name this state is tagged.
        """
        pass

    @abc.abstractmethod
    def get_all_states(self, name) -> Iterable[str]:
        """
            returns a list of all resource serialized states
        :param name:
        """
        pass

    @abc.abstractmethod
    def get_resource(self, name) -> Optional[ResourceRecord]:
        """
            return information about a specific resource
        :param name: resource name
        """
        pass

    @abc.abstractmethod
    def set_locked(self, resource_name):
        """
            sets the given resource as locked, meaning no one else can use this resource
            until it released.
        :param resource_name: the name of the resource
        """
        pass

    @abc.abstractmethod
    def set_released(self, resource_name):
        """
            release the given resource, meaning now some one else can lock the resource
            and use it.
        :param resource_name:
        """
        pass

    @abc.abstractmethod
    def get_all_resources(self) -> Set[str]:
        """
        :returns a list of all saved resources names
        """
        pass


def _get_class(module_name, class_name):
    module = importlib.import_module(module_name)
    if not hasattr(module, class_name):
        raise Exception("class {} is not in {}".format(class_name, module_name))
    logger.debug("reading class {} from module {}".format(class_name, module_name))
    cls = getattr(module, class_name)
    return cls


@dataclass
class _LabTopologyResourceInstance:
    name: str
    resource_class: Optional[Type]
    args: Optional[Any]
    kwargs: Optional[Any]
    instance: Optional[Any]


class LabResources:
    """
    A class for managing a persistent lab.
    """

    def __init__(self, persistence_db: PersistentLabDB) -> None:
        """
            A class for managing a persistent lab.
        :param persistence_db: a database that implemented PersistentLabDB interface.
                                the db can be the same as results db, or
                                a different one. This db can be shared across
                                executions and users,
        """
        super().__init__()
        self._resources: Dict[str, _LabTopologyResourceInstance] = {}
        self._persistent_db = persistence_db

    def resource_exist(self, name: str) -> bool:
        """
            True if resource exists in this lab
        :param name: resource unique name
        :return:
        """
        if name in self._resources:
            return True
        else:
            return self._persistent_db.get_resource(name) is not None

    def get_resource(
        self,
        name: str,
        experiment_args: Optional[List] = None,
        experiment_kwargs: Optional[Dict] = None,
    ):
        """
            returns an initialized instance of the resource,
            ready to execute actions.
        :param name: resource name
        :param experiment_args: arguments for initializing the resource,
                                as specified in register.
        :param experiment_kwargs: key word arguments for initializing the
                                resource, as specified in register.
        """
        if name not in self._resources:
            resource_instance = self._get_instance(
                name,
                experiment_args=experiment_args,
                experiment_kwargs=experiment_kwargs,
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
        self,
        name,
        experiment_args: Optional[List] = None,
        experiment_kwargs: Optional[Dict] = None,
    ):
        if name in self._resources:
            logger.debug(f"got device {name} from memory")
            return self._resources[name]
        else:
            record = self._persistent_db.get_resource(name)
            if record:
                args = jsonpickle.loads(record.args)
                kwargs = jsonpickle.loads(record.kwargs)
                resource_class = _get_class(record.module, record.class_name)
                if args is None:
                    args = []
                if kwargs is None:
                    kwargs = {}
                if experiment_args is None:
                    experiment_args = []
                if experiment_kwargs is None:
                    experiment_kwargs = {}
                combined_args = list(args) + list(experiment_args)
                combined_kwargs = {**kwargs, **experiment_kwargs}
                instance = resource_class(*combined_args, **combined_kwargs)
                if isinstance(instance, Resource):
                    instance.set_entropy_name(name)
                return _LabTopologyResourceInstance(
                    record.name,
                    resource_class,
                    combined_args,
                    combined_kwargs,
                    instance,
                )

        raise ResourceNotFound()

    def register_resource(
        self,
        name,
        resource_class: Type,
        args: Optional[List] = None,
        experiment_args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        experiment_kwargs: Optional[Dict] = None,
    ):
        """
            register a new resource and it's driver to the persistent lab.
            any combination of args and kwargs can be given.
        :param name: resource unique name
        :param resource_class: driver python class
        :param args: arguments for initializing the driver,
                    will be serialized and saved to the persistent lab
        :param experiment_args: more arguments for initializing the driver,
                                won't be save to the persistent lab, and
                                should be specified on every import.
        :param kwargs: key word arguments for initializing the driver,
                    will be serialized and saved to the persistent lab
        :param experiment_kwargs: more keyword arguments for initializing
                                the driver, won't be save to the persistent lab, and
                                should be specified on every import.
        """
        if name in self._resources or self.resource_exist(name):
            raise KeyError(f"instrument {name} already exist")
        self.update_resource(
            name, resource_class, args, experiment_args, kwargs, experiment_kwargs
        )

    def register_resource_if_not_exist(
        self,
        name,
        resource_class: Type,
        args: Optional[List] = None,
        experiment_args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        experiment_kwargs: Optional[Dict] = None,
    ):
        """
            register a new resource and it's driver to the persistent lab,
            only if it's not already exist.
            any combination of args and kwargs can be given.
        :param name: resource unique name
        :param resource_class: driver python class
        :param args: arguments for initializing the driver,
                    will be serialized and saved to the persistent lab
        :param experiment_args: more arguments for initializing the driver,
                                won't be save to the persistent lab, and
                                should be specified on every import.
        :param kwargs: key word arguments for initializing the driver,
                    will be serialized and saved to the persistent lab
        :param experiment_kwargs: more keyword arguments for initializing
                                the driver, won't be save to the persistent lab, and
                                should be specified on every import.
        """
        if name not in self._resources and not self.resource_exist(name):
            self.register_resource(
                name,
                resource_class,
                args,
                experiment_args=experiment_args,
                kwargs=kwargs,
                experiment_kwargs=experiment_kwargs,
            )

    def save_snapshot(self, resource_name: str, snapshot_name: str):
        """
        save the current state of the given resource to persistent lab.
        the state will be tagged with the snapshot name,
        and can be requested later.
        """
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

    def all_resources(self) -> Set[str]:
        """
            returns a set of all resource names in the lab
        :return:
        """
        return self._persistent_db.get_all_resources()

    def lock_resources(self, resources_names: Set[str]):
        """
            Lock all the given resources, so no one else will use them
        :param resources_names: a set of all resources to be locked
        """
        for resource_name in resources_names:
            resource_record = self._persistent_db.get_resource(resource_name)
            if resource_record:
                self._persistent_db.set_locked(resource_name)
            else:
                raise ResourceNotFound()

    def release_resources(self, resources_names: Set[str]):
        """
            Release the given resources for others to use
        :param resources_names: a set of all resources to be released
        """
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
        """
        returns information about the given resource
        """
        resource_record = self._persistent_db.get_resource(resource_name)
        if resource_record:
            return {"resource_class": resource_record.class_name}
        else:
            raise ResourceNotFound()

    def get_snapshot(self, resource_name: str, snapshot_name: str) -> str:
        """
            returns a serialized state of the given resource.
        :param resource_name: the name of the resource
        :param snapshot_name: the tag of the requested state
        :return:
        """
        return self._persistent_db.get_state(resource_name, snapshot_name)

    def remove_resource(self, resource_name):
        """
        remove the given resource from the persistent lab.
        """
        self._persistent_db.remove_resource(resource_name)

    def update_resource(
        self,
        name,
        resource_class: Type,
        args: Optional[List] = None,
        experiment_args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        experiment_kwargs: Optional[Dict] = None,
    ):
        """
            update the given resource with a new driver
        :param name: resource unique name
        :param resource_class: driver python class
        :param args: arguments for initializing the driver,
                    will be serialized and saved to the persistent lab
        :param experiment_args: more arguments for initializing the driver,
                                won't be save to the persistent lab, and
                                should be specified on every import.
        :param kwargs: key word arguments for initializing the driver,
                    will be serialized and saved to the persistent lab
        :param experiment_kwargs: more keyword arguments for initializing
                                the driver, won't be save to the persistent lab, and
                                should be specified on every import.
        """
        is_entropy_resource = issubclass(resource_class, Resource)
        if not is_entropy_resource:
            logger.warn(
                f"instrument {name} is not an entropylab Resource and"
                f" additional metadata won't be saved"
            )

        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}
        if experiment_args is None:
            experiment_args = []
        if experiment_kwargs is None:
            experiment_kwargs = {}
        combined_args = list(args) + list(experiment_args)
        combined_kwargs = {**kwargs, **experiment_kwargs}
        number_of_experiment_args = len(experiment_args)
        keys_of_experiment_kwargs = kwargs.keys()

        logger.debug(f"Initialize device {name}")
        resource_class(*combined_args, **combined_kwargs)
        serialized_args = jsonpickle.dumps(list(args))
        serialized_kwargs = jsonpickle.dumps(kwargs)
        try:
            source = inspect.getsource(inspect.getmodule(resource_class))
        except Exception:
            logger.warn(f"could not save source of {resource_class.__qualname__}")
            source = ""
        self._persistent_db.save_new_resource_driver(
            name,
            source,
            resource_class.__module__,
            resource_class.__qualname__,
            serialized_args,
            serialized_kwargs,
            number_of_experiment_args,
            keys_of_experiment_kwargs,
        )


class ExperimentResources:
    """
    A class for defining all resources needed for a specific experiment
    """

    def __init__(self, persistent_db: Optional[PersistentLabDB] = None) -> None:
        """
            A class for defining all resources needed for a specific experiment
        :param persistent_db: a persistent lab that this experiment runs in
                                if the db implmentation is also a DataWriter subclass,
                                this db will be used for results as well.
        """
        super().__init__()
        self._save_to_db = True
        self._results_db = None
        if isinstance(persistent_db, DataWriter):
            self._results_db = persistent_db
        if isinstance(persistent_db, PersistentLabDB):
            self._persistent_lab_connector = LabResources(persistent_db)
        self._private_results_db = None
        self._resources: Dict[str, Type] = {}
        self._local_resources: Dict[str, Any] = {}

    def _get_persistent_lab_connector(self) -> LabResources:
        if not self._persistent_lab_connector:
            raise Exception("persistent lab is not configured for this experiment")
        return self._persistent_lab_connector

    def register_private_results_db(self, db: DataWriter):
        """
            set a different results db for this experiment.
        :param db: the results db
        """
        self._private_results_db = db

    def pause_save_to_results_db(self):
        """
        don't save results in this experiment
        """
        logger.warn(
            "DB saving paused, results will be permanently lost on session close"
        )
        self._save_to_db = False

    def resume_save_to_results_db(self):
        """
        save results in this experiment
        """
        logger.warn("DB saving resumed, results will be safely saved to db")
        self._save_to_db = True

    def get_results_db(self) -> Optional[DataWriter]:
        """
        get the results db used in this experiment
        """
        if not self._save_to_db:
            return None
        if self._private_results_db:
            return self._private_results_db
        return self._results_db

    def get_results_reader(self) -> DataReader:
        """
        :returns a data reader from the current results db
        """
        db = self.get_results_db()
        if isinstance(db, DataReader):
            return db
        else:
            raise EntropyError(
                "the experiment db has not implemented DataReader abstract class"
            )

    def import_lab_resource(
        self,
        name: str,
        experiment_args: Optional[List] = None,
        experiment_kwargs: Optional[Dict] = None,
        snapshot_name: Optional[str] = None,
    ):
        """
            import a resource from the persistent lab to the current experiment
        :param name: resource name
        :param experiment_args: arguments for initializing the resource,
                                as specified in the persistent lab register.
        :param experiment_kwargs: key word arguments for initializing the
                                resource, as specified in the persistent lab register.
        :param snapshot_name: optional to import the resource in a specific state,
                              that was tagged with the snapshot name.
                              will only work if the resource implemented the
                              revert_to_snapshot function
        """
        if name in self._resources:
            raise Exception("can't import resource twice")
        if name in self._local_resources:
            raise Exception(
                "resource with the same name " "was already added as temp resource"
            )
        if self._get_persistent_lab_connector().resource_exist(name):
            resource = self._get_persistent_lab_connector().get_resource(
                name,
                experiment_args=experiment_args,
                experiment_kwargs=experiment_kwargs,
            )
            self._resources[name] = resource
            if snapshot_name:
                if isinstance(resource, Resource):
                    snap = self._get_persistent_lab_connector().get_snapshot(
                        name, snapshot_name
                    )
                    resource = self.get_resource(name)
                    resource.revert_to_snapshot(snap)
                else:
                    raise Exception(
                        "Resource is not an entropy resource and can't be reverted"
                    )
        else:
            raise Exception("Resource is not a part of the lab")

    def add_temp_resource(self, name: str, instance):
        """
            add a resource to this experiment, that is not part of the persistent lab.
        :param name: resource name
        :param instance: the python object
        """
        if name in self._resources:
            raise Exception(f"resource {name} is already imported from lab")
        if name not in self._local_resources:
            if isinstance(instance, Resource):
                instance.set_entropy_name(name)
            self._local_resources[name] = instance
        else:
            raise Exception(f"resource {name} already exist")

    def get_resource(self, name):
        """
            get the resource instance that was added/imported for this experiment
        :param name: resource name
        :return: resource instance
        """
        if name not in self._resources and name not in self._local_resources:
            raise KeyError(
                "cannot use a resource that wasn't added to experiment resources"
            )
        if name in self._resources:
            return self._get_persistent_lab_connector()._get_resource_if_already_initialized(
                name
            )
        if name in self._local_resources:
            return self._local_resources[name]

    def has_resource(self, name) -> bool:
        """
            True if the resource wass added or imported for this experiment
        :param name:
        :return:
        """
        return name in self._resources or name in self._local_resources

    def _lock_all_resources(self):
        if self._resources:
            self._get_persistent_lab_connector().lock_resources(
                set([resource_name for resource_name in self._resources])
            )

    def _release_all_resources(self):
        if self._resources:
            self._get_persistent_lab_connector().release_resources(
                set([resource_name for resource_name in self._resources])
            )
        if self._local_resources:
            for resource_name in self._local_resources:
                resource = self._local_resources[resource_name]
                if isinstance(resource, Instrument):
                    resource.teardown_driver()
                del resource

    def _serialize_resources_snapshot(self) -> Dict[str, str]:
        snapshots = {}
        for resource_name in self._resources:
            lab = self._get_persistent_lab_connector()
            resource = lab._get_resource_if_already_initialized(resource_name)
            if resource and isinstance(resource, Resource):
                snapshots[resource_name] = resource.snapshot(False)
        return snapshots

    def save_snapshot(self, resource_name: str, snapshot_name: str):
        """
        save the resource current state and tag it with the snapshot name
        """
        if resource_name not in self._resources:
            raise ResourceNotFound()
        self._get_persistent_lab_connector().save_snapshot(resource_name, snapshot_name)
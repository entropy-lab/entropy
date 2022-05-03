import os
import shutil
from pathlib import Path
from typing import TypeVar, Type, Tuple

import sqlalchemy.engine
from alembic import script, command
from alembic.config import Config
from alembic.runtime import migration
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from entropylab.api.errors import EntropyError
from entropylab.logger import logger
from entropylab.results_backend.sqlalchemy.model import Base, ResultTable, MetadataTable
from entropylab.results_backend.sqlalchemy.project import project_name, project_path
from entropylab.results_backend.sqlalchemy.storage import HDF5Storage, EntityType

T = TypeVar("T", bound=Base)

_SQL_ALCHEMY_MEMORY = ":memory:"
_ENTROPY_DIRNAME = ".entropy"
_DB_FILENAME = "entropy.db"
_HDF5_FILENAME = "entropy.hdf5"
_HDF5_DIRNAME = "hdf5"


class _DbInitializer:
    def __init__(self, path: str, echo=False):
        """
        :param path: path to directory containing Entropy project
        :param echo: if True, the database engine will log all statements
        """
        self._validate_path(path)

        if path is None or path == _SQL_ALCHEMY_MEMORY:
            logger.debug("_DbInitializer is in in-memory mode")
            self._storage = HDF5Storage()
            self._engine = create_engine("sqlite:///" + _SQL_ALCHEMY_MEMORY, echo=echo)
            self._alembic_util = _AlembicUtil(self._engine)
        else:
            logger.debug("_DbInitializer is in project directory mode")
            creating_new = not os.path.isdir(path)
            entropy_dir_path = os.path.join(path, _ENTROPY_DIRNAME)
            os.makedirs(entropy_dir_path, exist_ok=True)
            logger.debug(f"Entropy directory is at: {entropy_dir_path}")

            db_file_path = os.path.join(entropy_dir_path, _DB_FILENAME)
            logger.debug(f"DB file is at: {db_file_path}")

            hdf5_dir_path = os.path.join(entropy_dir_path, _HDF5_DIRNAME)
            logger.debug(f"hdf5 directory is at: {hdf5_dir_path}")

            self._engine = create_engine("sqlite:///" + db_file_path, echo=echo)
            self._storage = HDF5Storage(hdf5_dir_path)
            self._alembic_util = _AlembicUtil(self._engine)
            if creating_new:
                self._print_project_created(path)

    def init_db(self) -> Tuple[sqlalchemy.engine.Engine, HDF5Storage]:
        """If the database is empty, initializes it with the most up to date schema.
        If the database is not empty, ensures that is up to date."""
        if self._db_is_empty():
            Base.metadata.create_all(self._engine)
            self._alembic_util.stamp_head()
        else:
            if not self._alembic_util.db_is_up_to_date():
                path = str(self._engine.url)
                raise EntropyError(
                    f"The database at {path} is not up-to-date. Update the database "
                    "using the Entropy CLI command `entropy upgrade`. "
                    "* Before upgrading be sure to back up your database to a safe "
                    "place *."
                )
        return self._engine, self._storage

    @staticmethod
    def _print_project_created(path):
        print(
            f"New Entropy project '{project_name(path)}' created at "
            f"'{project_path(path)}'"
        )

    @staticmethod
    def _validate_path(path):
        if path is None or path == _SQL_ALCHEMY_MEMORY:
            return
        if path is not None and Path(path).suffix == ".db":
            logger.error(
                "_DbInitializer given path to a sqlite database. This is deprecated."
            )
            raise EntropyError(
                "Providing the SqlAlchemyDB() constructor with a path to a sqlite "
                "database is deprecated. You should instead provide the path to a "
                "directory containing an Entropy project.\n"
                "To upgrade your existing sqlite database file to an Entropy project "
                "please use the Entropy CLI command: `entropy upgrade`.\n"
                "* Before upgrading be sure to back up your database to a safe place *."
            )
        if path is not None and os.path.isfile(path):
            logger.error(
                "_DbInitializer provided with path to a file, not a directory."
            )
            raise RuntimeError(
                "SqlAlchemyDB() constructor provided with a path to a file but "
                "expects the path to an Entropy project directory"
            )

    def _db_is_empty(self) -> bool:
        cursor = self._engine.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table'"
        )
        return len(cursor.fetchall()) == 0


class _DbUpgrader:
    def __init__(self, path: str, echo=False) -> None:
        self._path = path
        self._echo = echo
        self._engine = None
        self._storage = None
        self._alembic_util = None

    def upgrade_db(self) -> None:
        old_global_hdf5_file_path = None
        if self._path is None or self._path == _SQL_ALCHEMY_MEMORY:
            logger.debug("_DbUpgrader is in in-memory mode")
            self._storage = HDF5Storage()
            self._engine = create_engine(
                "sqlite:///" + _SQL_ALCHEMY_MEMORY, echo=self._echo
            )
        else:
            logger.debug("_DbUpgrader is in project directory mode")
            if not os.path.exists(self._path):
                raise EntropyError(f"No Entropy project exists at '{self._path}'")
            if self._path_is_to_db():
                self._convert_to_project()
            entropy_dir_path = os.path.join(self._path, _ENTROPY_DIRNAME)
            db_file_path = os.path.join(entropy_dir_path, _DB_FILENAME)
            hdf5_dir_path = os.path.join(entropy_dir_path, _HDF5_DIRNAME)
            old_global_hdf5_file_path = os.path.join(entropy_dir_path, _HDF5_FILENAME)
            self._engine = create_engine("sqlite:///" + db_file_path, echo=self._echo)
            self._storage = HDF5Storage(hdf5_dir_path)
        self._alembic_util = _AlembicUtil(self._engine)
        self._alembic_util.upgrade()
        if old_global_hdf5_file_path and os.path.isfile(old_global_hdf5_file_path):
            # old, global hdf5 file exists so migrate from it to new "per experiment"
            # hdf5 files
            self._storage.migrate_from_per_project_hdf5_to_per_experiment_hdf5_files(
                old_global_hdf5_file_path
            )
        else:
            # old, global hdf5 file does not exist so migrate directly from DB to new
            # "per experiment" hdf5 files
            self._migrate_results_from_db_to_hdf5()
            self._migrate_metadata_from_db_to_hdf5()

    def _path_doesnt_exist(self):
        return not os.path.isdir(self._path) and not os.path.isfile(self._path)

    def _path_is_to_db(self):
        return (
            self._path is not None
            and (Path(self._path).suffix == ".db")
            or os.path.isfile(self._path)
        )

    def _convert_to_project(self):
        # old
        old_db_file_path = self._path
        old_hdf5_file_path = old_db_file_path.replace(".db", ".hdf5")
        # new
        new_project_dir_path = os.path.splitext(old_db_file_path)[0]
        new_entropy_dir_path = os.path.join(new_project_dir_path, _ENTROPY_DIRNAME)
        new_db_file_path = os.path.join(new_entropy_dir_path, _DB_FILENAME)
        new_hdf5_file_path = os.path.join(new_entropy_dir_path, _HDF5_FILENAME)
        # convert
        os.makedirs(new_entropy_dir_path, exist_ok=True)
        shutil.move(old_db_file_path, new_db_file_path)
        if os.path.exists(old_hdf5_file_path):
            shutil.move(old_hdf5_file_path, new_hdf5_file_path)
        self._path = new_project_dir_path
        print(
            f"Converted db file at {old_db_file_path} to project directory "
            f"at {new_project_dir_path}"
        )

    def _migrate_results_from_db_to_hdf5(self) -> None:
        self._migrate_rows_from_db_to_hdf5(EntityType.RESULT, ResultTable)

    def _migrate_metadata_from_db_to_hdf5(self) -> None:
        self._migrate_rows_from_db_to_hdf5(EntityType.METADATA, MetadataTable)

    def _migrate_rows_from_db_to_hdf5(self, entity_type: EntityType, table: Type[T]):
        logger.debug(f"Migrating {entity_type.name} rows from sqlite to hdf5")
        session_maker = sessionmaker(bind=self._engine)
        with session_maker() as session:
            rows = session.query(table).filter(table.saved_in_hdf5.is_(False)).all()
            if len(rows) == 0:
                logger.debug(f"No {entity_type.name} rows need migrating. Done")
            else:
                logger.debug(f"Found {len(rows)} {entity_type.name} rows to migrate")
                self._storage.migrate_rows(entity_type, rows)
                logger.debug(f"Migrated {len(rows)} {entity_type.name} to hdf5")
                for row in rows:
                    row.saved_in_hdf5 = True
                session.commit()
                logger.debug(
                    f"Marked all {entity_type.name} rows in sqlite as `saved_in_hdf5`."
                    f" Done"
                )


class _AlembicUtil:
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self._engine = engine

    @staticmethod
    def _abs_path_to(rel_path: str) -> str:
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent
        return os.path.join(source_dir, rel_path)

    def upgrade(self) -> None:
        with self._engine.connect() as connection:
            alembic_cfg = self._alembic_build_config(connection)
            command.upgrade(alembic_cfg, "head")

    def stamp_head(self) -> None:
        with self._engine.connect() as connection:
            alembic_cfg = self._alembic_build_config(connection)
            command.stamp(alembic_cfg, "head")

    def db_is_up_to_date(self) -> bool:
        script_location = self._abs_path_to("alembic")
        script_ = script.ScriptDirectory(script_location)
        with self._engine.begin() as conn:
            context = migration.MigrationContext.configure(conn)
            db_version = context.get_current_revision()
            latest_version = script_.get_current_head()
            return db_version == latest_version

    def _alembic_build_config(self, connection: sqlalchemy.engine.Connection) -> Config:
        config_location = self._abs_path_to("alembic.ini")
        script_location = self._abs_path_to("alembic")
        alembic_cfg = Config(config_location)
        alembic_cfg.set_main_option("script_location", script_location)
        """
        Modified by @urig to share a single engine across multiple (typically in-memory)
        connections, based on this cookbook recipe:
        https://alembic.sqlalchemy.org/en/latest/cookbook.html#connection-sharing
        """
        alembic_cfg.set_main_option("sqlalchemy.url", "sqlite:///:memory:")
        alembic_cfg.attributes["connection"] = connection  # overrides dummy url above
        return alembic_cfg

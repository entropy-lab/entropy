import os
from pathlib import Path
from typing import TypeVar, Type

from alembic import script, command
from alembic.config import Config
from alembic.runtime import migration
from sqlalchemy import create_engine
import sqlalchemy.engine
from sqlalchemy.orm import sessionmaker

from entropylab.logger import logger
from entropylab.results_backend.sqlalchemy.model import Base, ResultTable, MetadataTable
from entropylab.results_backend.sqlalchemy.storage import HDF5Storage, EntityType

_SQL_ALCHEMY_MEMORY = ":memory:"

T = TypeVar("T", bound=Base)


class _DbInitializer:
    def __init__(self, path: str, echo=False):
        if path is None:
            path = _SQL_ALCHEMY_MEMORY
        if path == _SQL_ALCHEMY_MEMORY:
            self._storage = HDF5Storage()
        else:
            self._create_parent_dirs(path)
            hdf5_path = Path(path).with_suffix(".hdf5")
            self._storage = HDF5Storage(hdf5_path)
        dsn = "sqlite:///" + path
        self._engine = create_engine(dsn, echo=echo)

    def init_db(self) -> sqlalchemy.engine.Engine:
        if self._db_is_empty():
            Base.metadata.create_all(self._engine)
            self._alembic_stamp_head()
        else:
            if not self._db_is_up_to_date():
                path = str(self._engine.url)
                raise RuntimeError(
                    f"The database at {path} is not up-to-date. Update the database "
                    f"using the function entropylab.results_backend.sqlalchemy"
                    ".upgrade_db(). * Be sure to back up your database to a safe place "
                    "before upgrading it *."
                )
        return self._engine, self._storage

    def upgrade_db(self) -> None:
        self._alembic_upgrade()
        self._migrate_results_to_hdf5()
        self._migrate_metadata_to_hdf5()

    @staticmethod
    def _create_parent_dirs(path) -> None:
        dirname = os.path.dirname(path)
        if dirname and dirname != "" and dirname != ".":
            os.makedirs(dirname, exist_ok=True)

    def _db_is_empty(self) -> bool:
        cursor = self._engine.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table'"
        )
        return len(cursor.fetchall()) == 0

    def _db_is_up_to_date(self) -> bool:
        script_location = self._abs_path_to("alembic")
        script_ = script.ScriptDirectory(script_location)
        with self._engine.begin() as conn:
            context = migration.MigrationContext.configure(conn)
            db_version = context.get_current_revision()
            latest_version = script_.get_current_head()
            return db_version == latest_version

    @staticmethod
    def _abs_path_to(rel_path: str) -> str:
        source_path = Path(__file__).resolve()
        source_dir = source_path.parent
        return os.path.join(source_dir, rel_path)

    def _alembic_upgrade(self) -> None:
        with self._engine.connect() as connection:
            alembic_cfg = self._alembic_build_config(connection)
            command.upgrade(alembic_cfg, "head")

    def _alembic_stamp_head(self) -> None:
        with self._engine.connect() as connection:
            alembic_cfg = self._alembic_build_config(connection)
            command.stamp(alembic_cfg, "head")

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

    def _migrate_results_to_hdf5(self) -> None:
        self._migrate_rows_to_hdf5(EntityType.RESULT, ResultTable)

    def _migrate_metadata_to_hdf5(self) -> None:
        self._migrate_rows_to_hdf5(EntityType.METADATA, MetadataTable)

    def _migrate_rows_to_hdf5(self, entity_type: EntityType, table: Type[T]):
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
                    f"Marked all {entity_type.name} rows in sqlite as `saved_in_hdf5`. Done"
                )

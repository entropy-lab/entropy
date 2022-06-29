import os
from pathlib import Path

import sqlalchemy.engine
from alembic import command, script, op
from alembic.config import Config
from alembic.runtime import migration

from entropylab.pipeline.results_backend.sqlalchemy.project import param_store_file_path


class AlembicUtil:
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self._engine = engine

    @staticmethod
    def get_param_store_file_path():
        conn = op.get_bind()
        project_path = os.path.abspath(
            os.path.join(os.path.dirname(conn.engine.url.database), "..")
        )
        return param_store_file_path(project_path)

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
        script_location = self._abs_path_to("")
        script_ = script.ScriptDirectory(script_location)
        with self._engine.begin() as conn:
            context = migration.MigrationContext.configure(conn)
            db_version = context.get_current_revision()
            latest_version = script_.get_current_head()
            return db_version == latest_version

    def _alembic_build_config(self, connection: sqlalchemy.engine.Connection) -> Config:
        config_location = self._abs_path_to("../alembic.ini")
        script_location = self._abs_path_to("")
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

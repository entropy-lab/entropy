"""Deleting results and metadata from sqlite

Revision ID: 7fa75ca1263f
Revises: 06140c96c8c4
Create Date: 2022-06-16 13:26:06.576200+00:00

"""
import re
import shutil

from alembic import op

# revision identifiers, used by Alembic.
from entropylab.logger import logger

revision = "7fa75ca1263f"
down_revision = "06140c96c8c4"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    db_path = conn.engine.url.database
    if db_path == ":memory:":
        return
    backup_path = get_backup_path(db_path)

    logger.debug(f"Backing up database to [{backup_path}] ")
    shutil.copy2(db_path, backup_path)

    logger.debug("Deleting Result records that are saved in HDF5")
    op.execute("DELETE FROM Results WHERE saved_in_hdf5=1;")

    logger.debug("Deleting ExperimentMetadata records that are saved in HDF5")
    op.execute("DELETE FROM ExperimentMetadata WHERE saved_in_hdf5=1;")

    logger.debug("Vacuuming DB to reduce its size")
    with op.get_context().autocommit_block():
        op.execute("VACUUM")


def downgrade():
    conn = op.get_bind()
    db_path = conn.engine.url.database
    if db_path == ":memory:":
        return
    backup_path = get_backup_path(db_path)
    downgraded_db_path = get_downgraded_db_path(db_path)
    logger.debug(
        f"Attempting to downgrade Entropy DB {db_path} "
        f"using backup file {backup_path}"
    )
    shutil.move(db_path, downgraded_db_path)
    shutil.move(backup_path, db_path)


def get_backup_path(db_path: str) -> str:
    return re.sub(".db$", f".bak.{revision}.db", db_path)


def get_downgraded_db_path(db_path: str) -> str:
    return re.sub(".db$", f".downgraded.{revision}.db", db_path)

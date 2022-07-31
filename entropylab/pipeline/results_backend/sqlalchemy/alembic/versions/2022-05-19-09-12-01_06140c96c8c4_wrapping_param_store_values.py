"""wrapping_param_store_values

Revision ID: 06140c96c8c4
Revises: 9ffd2ba0d5bf
Create Date: 2022-05-19 09:12:01.569825+00:00

"""

import shutil

from entropylab.logger import logger
from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.params.persistence.migrations import \
    migrate_param_store_0_1_to_0_2
from entropylab.pipeline.params.persistence.tinydb.tinydbpersistence import \
    _set_version
from entropylab.pipeline.results_backend.sqlalchemy.alembic.alembic_util import (
    AlembicUtil,
)

# revision identifiers, used by Alembic.
revision = "06140c96c8c4"
down_revision = "9ffd2ba0d5bf"
branch_labels = None
depends_on = None


def upgrade():
    path = str(AlembicUtil.get_param_store_file_path())
    logger.debug(
        f"Attempting to migrate InProcessParamStore file {path} from v0.1 to v0.2"
    )
    try:
        migrate_param_store_0_1_to_0_2(path, revision)
        _set_version(path, "0.2", revision)
    except EntropyError as ee:
        logger.warning(str(ee))
    logger.debug("Done migrating from v0.1 to v0.2")


def downgrade():
    path = AlembicUtil.get_param_store_file_path()
    backup_json_file_path = path.replace(".json", ".json.{revision}.bak")
    downgraded_json_file_path = path.replace(".json", ".json.{revision}.downgraded")
    logger.debug(
        f"Attempting to downgrade InProcessParamStore file {path} from v0.2 "
        f"to v0.1 using backup file {backup_json_file_path}"
    )
    shutil.move(path, downgraded_json_file_path)
    shutil.move(backup_json_file_path, path)
    logger.debug("Done downgrading from v0.2 to v0.1")

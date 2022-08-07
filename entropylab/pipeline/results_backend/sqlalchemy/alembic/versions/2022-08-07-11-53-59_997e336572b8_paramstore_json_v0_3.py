"""ParamStore JSON v0.3

Revision ID: 997e336572b8
Revises: 09f3b5a1689c
Create Date: 2022-08-07 11:53:59.596941+00:00

"""

import shutil

from entropylab.logger import logger
from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.params.persistence.migrations import \
    migrate_param_store_0_2_to_0_3
from entropylab.pipeline.params.persistence.tinydb.tinydbpersistence import set_version
from entropylab.pipeline.results_backend.sqlalchemy.alembic.alembic_util import (
    AlembicUtil,
)

# revision identifiers, used by Alembic.
revision = "997e336572b8"
down_revision = "09f3b5a1689c"
branch_labels = None
depends_on = None


def upgrade():
    path = str(AlembicUtil.get_param_store_file_path())
    logger.debug(f"Attempting to migrate ParamStore file {path} from v0.2 to v0.3")
    try:
        migrate_param_store_0_2_to_0_3(path, revision)
        set_version(path, "0.3", revision)
    except EntropyError as ee:
        logger.warning(str(ee))
    logger.debug("Done migrating from v0.2 to v0.3")


def downgrade():
    path = AlembicUtil.get_param_store_file_path()
    backup_json_file_path = path.replace(".json", ".json.{revision}.bak")
    downgraded_json_file_path = path.replace(".json", ".json.{revision}.downgraded")
    logger.debug(
        f"Attempting to downgrade ParamStore file {path} from v0.3 "
        f"to v0.2 using backup file {backup_json_file_path}"
    )
    shutil.move(path, downgraded_json_file_path)
    shutil.move(backup_json_file_path, path)
    logger.debug("Done downgrading from v0.3 to v0.2")

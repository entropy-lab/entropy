"""Fixing Param qualified names as they appear in the ParamStore JSON file

Revision ID: 09f3b5a1689c
Revises: 273a9fae6206
Create Date: 2022-06-28 12:13:39.743933+00:00

"""
import shutil

from entropylab.logger import logger
from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.api.in_process_param_store import (
    fix_param_qualified_name,
    _set_version,
)
from entropylab.pipeline.results_backend.sqlalchemy.alembic.alembic_util import (
    AlembicUtil,
)

# revision identifiers, used by Alembic.

revision = "09f3b5a1689c"
down_revision = "273a9fae6206"
branch_labels = None
depends_on = None


def upgrade():
    path = str(AlembicUtil.get_param_store_file_path())
    logger.debug(
        f"Starting to fix Param qualified name in InProcessParamStore file [{path}]"
    )
    try:
        fix_param_qualified_name(path, revision)
        _set_version(path, 0.2, revision)
    except EntropyError as ee:
        logger.warning(str(ee))
    logger.debug("Done fixing Param qualified name.")


def downgrade():
    path = AlembicUtil.get_param_store_file_path()
    backup_json_file_path = path.replace(".json", ".json.{revision}.bak")
    downgraded_json_file_path = path.replace(".json", ".json.{revision}.downgraded")
    logger.debug(
        f"Attempting to downgrade InProcessParamStore file {path} from revision "
        f"{revision} to {down_revision} using backup file {backup_json_file_path}"
    )
    shutil.move(path, downgraded_json_file_path)
    shutil.move(backup_json_file_path, path)
    logger.debug(f"Done downgrading from {revision} to {down_revision}")

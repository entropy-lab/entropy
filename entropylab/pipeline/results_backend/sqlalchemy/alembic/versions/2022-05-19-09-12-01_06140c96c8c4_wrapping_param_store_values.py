"""wrapping_param_store_values

Revision ID: 06140c96c8c4
Revises: 9ffd2ba0d5bf
Create Date: 2022-05-19 09:12:01.569825+00:00

"""

import logging
import os
import shutil

from alembic import op

from entropylab.pipeline.api.errors import EntropyError
from entropylab.pipeline.api.in_process_param_store import (
    migrate_param_store_0_1_to_0_2,
)
from entropylab.pipeline.results_backend.sqlalchemy.project import param_store_file_path

# revision identifiers, used by Alembic.
revision = "06140c96c8c4"
down_revision = "9ffd2ba0d5bf"
branch_labels = None
depends_on = None


def upgrade():
    logger = logging.getLogger(__name__)
    conn = op.get_bind()
    project_path = os.path.abspath(
        os.path.join(os.path.dirname(conn.engine.url.database), "..")
    )
    json_file_path = param_store_file_path(project_path)
    logger.debug(
        f"Attempting to migrate InProcessParamStore file {json_file_path} from v0.1 to "
        f"v0.2"
    )
    try:
        migrate_param_store_0_1_to_0_2(str(json_file_path))
    except EntropyError as ee:
        logger.warning(str(ee))


def downgrade():
    logger = logging.getLogger(__name__)
    conn = op.get_bind()
    project_path = os.path.abspath(
        os.path.join(os.path.dirname(conn.engine.url.database), "..")
    )
    json_file_path = str(param_store_file_path(project_path))
    backup_json_file_path = json_file_path.replace(".json", ".bak.json")
    downgraded_json_file_path = json_file_path.replace(".json", ".downgraded.json")
    logger.debug(
        f"Attempting to downgrade  InProcessParamStore file {json_file_path} from v0.2 "
        f"to v0.1 using backup file {backup_json_file_path}"
    )
    shutil.move(json_file_path, downgraded_json_file_path)
    shutil.move(backup_json_file_path, json_file_path)
    logger.debug(
        f"Attempting to migrate InProcessParamStore file {json_file_path} from v0.1 to "
        f"v0.2"
    )

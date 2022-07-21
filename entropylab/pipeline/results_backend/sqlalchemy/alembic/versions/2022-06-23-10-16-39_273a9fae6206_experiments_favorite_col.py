"""experiments_favorite_col

Revision ID: 273a9fae6206
Revises: 7fa75ca1263f
Create Date: 2022-06-23 10:16:39.441194+00:00

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "273a9fae6206"
down_revision = "7fa75ca1263f"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "Experiments",
        sa.Column(
            "favorite",
            sa.Boolean(),
            nullable=False,
            default=False,
            server_default=text("False"),
        ),
    )


def downgrade():
    op.drop_column("Experiments", "favorite")

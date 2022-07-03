"""Matplotlib figures

Revision ID: da8d38e19ff8
Revises: 09f3b5a1689c
Create Date: 2022-07-03 08:56:23.627973+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from sqlalchemy.engine import Inspector

# revision identifiers, used by Alembic.
revision = "da8d38e19ff8"
down_revision = "09f3b5a1689c"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names()
    if "MatplotlibFigures" not in tables:
        op.create_table(
            "MatplotlibFigures",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("experiment_id", sa.Integer(), nullable=False),
            sa.Column("figure", sa.String(), nullable=False),
            sa.Column("time", sa.DATETIME(), nullable=False),
            sa.ForeignKeyConstraint(
                ["experiment_id"], ["Experiments.id"], ondelete="CASCADE"
            ),
            sa.PrimaryKeyConstraint("id"),
        )


def downgrade():
    op.drop_table("MatplotlibFigures")

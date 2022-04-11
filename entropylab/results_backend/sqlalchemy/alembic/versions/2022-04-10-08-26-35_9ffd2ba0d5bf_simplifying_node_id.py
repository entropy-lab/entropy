"""simplifying node id

Revision ID: 9ffd2ba0d5bf
Revises: f1ada2484fe2
Create Date: 2022-04-10 08:26:35.076466+00:00

"""
from alembic import op

# from sqlalchemy import Column, Integer

# revision identifiers, used by Alembic.
revision = "9ffd2ba0d5bf"
down_revision = "f1ada2484fe2"
branch_labels = None
depends_on = None

NodeTable = "Nodes"


def upgrade():
    op.execute("""DROP TABLE IF EXISTS "Nodes_new";""")

    op.execute(
        """CREATE TABLE "Nodes_new" (
        id INTEGER PRIMARY KEY,
        experiment_id INTEGER NOT NULL,
        stage_id INTEGER NOT NULL,
        label VARCHAR,
        start DATETIME NOT NULL,
        is_key_node BOOLEAN,
        FOREIGN KEY(experiment_id) REFERENCES "Experiments" (id) ON DELETE CASCADE);"""
    )

    op.execute(
        """INSERT INTO Nodes_new
        (experiment_id, stage_id, label, start, is_key_node)
        SELECT experiment_id, id, label, start, is_key_node
        FROM Nodes;"""
    )

    op.execute("""ALTER TABLE "Nodes" RENAME TO "Nodes_old";""")

    op.execute("""ALTER TABLE "Nodes_new" RENAME TO "Nodes";""")


def downgrade():
    op.execute("""ALTER TABLE "Nodes" RENAME TO "Nodes_new";""")

    op.execute("""ALTER TABLE "Nodes_old" RENAME TO "Nodes";""")

    pass

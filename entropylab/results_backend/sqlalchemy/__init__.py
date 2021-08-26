from . import db
from .db import SqlAlchemyDB

from entropylab.results_backend.sqlalchemy.db_initializer import _DbInitializer


def upgrade_db(path: str):
    """Upgrades an Entropy SQLite database to the latest schema version

    * Be sure to back up your database to a safe place before upgrading it *.

    :param path: The path to the SQLite database to be upgraded
    """
    _DbInitializer(path).upgrade_db()

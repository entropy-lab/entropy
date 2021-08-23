from . import db
from .db import SqlAlchemyDB

from entropylab.results_backend.sqlalchemy.db_initializer import _DbInitializer


def upgrade_db(path: str):
    _DbInitializer(path).upgrade_db()

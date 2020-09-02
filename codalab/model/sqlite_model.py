"""
SQLite is a subclass of BundleModel that stores metadata in an in-memory
SQLite database. Only used for testing purposes.
"""
import array
from sqlalchemy import create_engine, event, exc
from sqlalchemy.pool import Pool, StaticPool

from codalab.model.bundle_model import BundleModel
from codalab.common import UsageError


class SQLiteModel(BundleModel):
    def __init__(self, default_user_info, root_user_id, system_user_id):
        # Use an in-memory database in multiple threads -- see
        # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#threading-pooling-behavior
        engine = create_engine(
            'sqlite:///:memory:',
            strategy='threadlocal',
            encoding='utf-8',
            connect_args={'check_same_thread': False},
            poolclass=StaticPool,
        )
        super(SQLiteModel, self).__init__(engine, default_user_info, root_user_id, system_user_id)

"""
SQLite is a subclass of BundleModel that stores metadata on a SQLite
server. Only used for testing with in-memory databases.
"""
import array
from sqlalchemy import create_engine, event, exc
from sqlalchemy.pool import Pool

from codalab.model.bundle_model import BundleModel
from codalab.common import UsageError


class SQLiteModel(BundleModel):
    def __init__(self, engine_url, default_user_info, root_user_id, system_user_id):
        if not engine_url.startswith('sqlite://'):
            raise UsageError('Engine URL should start with sqlite://')
        engine = create_engine(
            engine_url, strategy='threadlocal', pool_size=20, pool_recycle=3600, encoding='utf-8',
        )
        super(SQLiteModel, self).__init__(engine, default_user_info, root_user_id, system_user_id)

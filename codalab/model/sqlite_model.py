'''
SQLiteModel is a subclass of BundleModel that stores metadata in a sqlite3
database in a local file in the CodaLab home directory.
'''
import os
from sqlalchemy import create_engine

from codalab.model.bundle_model import BundleModel


class SQLiteModel(BundleModel):
    def __init__(self, engine_url):
        if not engine_url.startswith('sqlite:///'):
            raise UsageError('Engine URL should start with sqlite:///')

        engine = create_engine(engine_url, strategy='threadlocal')
        super(SQLiteModel, self).__init__(engine)

    def encode_str(self, value):
        return value
    def decode_str(self, value):
        return value

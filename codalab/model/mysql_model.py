'''
MySQLModel is a subclass of BundleModel that stores metadata on a MySQL
server that it connects to with the given connect parameters.
'''
from sqlalchemy import create_engine

from codalab.model.bundle_model import BundleModel
from codalab.common import (
    UsageError,
)

class MySQLModel(BundleModel):
    def __init__(self, engine_url):
        if not engine_url.startswith('mysql://'):
            raise UsageError('Engine URL should start with %s' % engine_url)
        engine = create_engine(engine_url, strategy='threadlocal')
        super(MySQLModel, self).__init__(engine)

    def do_multirow_insert(self, connection, table, values):
        # MySQL allows for more efficient multi-row insertions.
        if values:
            connection.execute(table.insert().values(values))

    def encode_str(self, value):
        return value.encode('utf-8')
    def decode_str(self, value):
        return value.decode('utf-8')

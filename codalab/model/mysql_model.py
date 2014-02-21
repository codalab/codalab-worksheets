'''
MySQLModel is a subclass of BundleModel that stores metadata on a MySQL
server that it connects to with the given connect parameters.
'''
from sqlalchemy import create_engine

from codalab.model.bundle_model import BundleModel


class MySQLModel(BundleModel):
    def __init__(self, username, password, address, database):
        engine_url = 'mysql://%s%s@%s/%s' % (
          username,
          (':' + password if password else ''),
          address,
          database,
        )
        engine = create_engine(engine_url, strategy='threadlocal')
        super(MySQLModel, self).__init__(engine)

    def do_multirow_insert(self, connection, table, values):
        # MySQL allows for more efficient multi-row insertions.
        if values:
            connection.execute(table.insert().values(values))

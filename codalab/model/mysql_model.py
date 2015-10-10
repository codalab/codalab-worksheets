'''
MySQLModel is a subclass of BundleModel that stores metadata on a MySQL
server that it connects to with the given connect parameters.
'''
import os

from sqlalchemy import (
    create_engine,
    event,
    exc,
)
from sqlalchemy.pool import Pool

from codalab.model.bundle_model import BundleModel
from codalab.common import (
    UsageError,
)

@event.listens_for(Pool, "checkout")
def ping_connection(dbapi_connection, connection_record, connection_proxy):
    '''
    Checkout listener that ping the connection to check if it is alive.
    If there is a problem (i.e. "MySQL server has gone away"), will raise a
    sqlalchemy.DisconnectionError, which is caught internally by SQLALchemy and
    forces up to three retries from the connection pool.

    Adapted from:
    http://stackoverflow.com/questions/18054224/python-sqlalchemy-mysql-server-has-gone-away
    http://docs.sqlalchemy.org/en/latest/core/pooling.html#disconnect-handling-optimistic
    '''
    try:
        try:
            # MySQLdb exposes a non-standard ping() method
            dbapi_connection.ping(False)
        except (AttributeError, TypeError):
            # Use standard DB-API calls
            cursor = dbapi_connection.cursor()
            cursor.execute("SELECT 1")
    except dbapi_connection.OperationalError as e:
        if e.args[0] in (2006, 2013, 2014, 2045, 2055):
            raise exc.DisconnectionError()
        else:
            raise

@event.listens_for(Pool, "connect")
def set_connection_pid(dbapi_connection, connection_record):
    '''
    Save the pid of this process in the connection record.
    '''
    connection_record.info['pid'] = os.getpid()

@event.listens_for(Pool, "checkout")
def invalidate_foreign_connections(dbapi_connection, connection_record, connection_proxy):
    '''
    Invalidate connection if its pid doesn't match this process.
    Guarantees that multiple processes do not share the same connections.

    From this recipe for multiprocessing:
    http://docs.sqlalchemy.org/en/rel_0_9/core/pooling.html#using-connection-pools-with-multiprocessing
    '''
    pid = os.getpid()
    if connection_record.info['pid'] != pid:
        connection_record.connection = connection_proxy.connection = None
        raise exc.DisconnectionError(
                "Connection record belongs to pid %s, "
                "attempting to check out in pid %s" %
                (connection_record.info['pid'], pid)
        )

class MySQLModel(BundleModel):
    def __init__(self, engine_url):
        if not engine_url.startswith('mysql://'):
            raise UsageError('Engine URL should start with %s' % engine_url)
        engine = create_engine(engine_url, strategy='threadlocal', pool_recycle=3600)
        event.listen(engine, 'checkout', ping_connection)
        super(MySQLModel, self).__init__(engine)

    def do_multirow_insert(self, connection, table, values):
        # MySQL allows for more efficient multi-row insertions.
        if values:
            connection.execute(table.insert().values(values))

    def encode_str(self, value):
        return value.encode('utf-8')
    def decode_str(self, value):
        return value.decode('utf-8')

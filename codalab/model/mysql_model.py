"""
MySQLModel is a subclass of BundleModel that stores metadata on a MySQL
server that it connects to with the given connect parameters.
"""
import array
from sqlalchemy import create_engine, event, exc
from sqlalchemy.pool import Pool

from codalab.model.bundle_model import BundleModel
from codalab.common import UsageError


@event.listens_for(Pool, "checkout")
def ping_connection(dbapi_connection, connection_record, connection_proxy):
    """
    Checkout listener that ping the connection to check if it is alive.
    If there is a problem (i.e. "MySQL server has gone away"), will raise a
    sqlalchemy.DisconnectionError, which is caught internally by SQLALchemy and
    forces up to three retries from the connection pool.

    Adapted from:
    http://stackoverflow.com/questions/18054224/python-sqlalchemy-mysql-server-has-gone-away
    http://docs.sqlalchemy.org/en/latest/core/pooling.html#disconnect-handling-optimistic
    """
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


class MySQLModel(BundleModel):
    def __init__(self, engine_url, default_user_info, root_user_id, system_user_id):
        if not engine_url.startswith('mysql://'):
            raise UsageError('Engine URL should start with mysql://')
        engine = create_engine(
            engine_url,
            strategy='threadlocal',
            pool_size=20,
            max_overflow=100,
            pool_recycle=3600,
            encoding='utf-8',
        )
        super(MySQLModel, self).__init__(engine, default_user_info, root_user_id, system_user_id)

    def do_multirow_insert(self, connection, table, values):
        # MySQL allows for more efficient multi-row insertions.
        if values:
            connection.execute(table.insert().values(values))

    # TODO: Remove these methods below when all appropriate table columns have
    # been converted to the appropriate types that perform automatic encoding.
    # (See tables.py for more details.)
    # These two methods are currently used for: worksheet title, body
    # Please update the line above if more fields are using this encoding hack
    # These two methods are needed right now for unicode support because the
    # database configuration doesn't support storing unicode directly.
    # To correctly use it, we should find all codes on the backend side that
    # actually stores/gets the encoded field

    def encode_str(self, value):
        return value.encode()

    def decode_str(self, value):
        return array.array('B', [ord(char) for char in value]).tostring().decode()

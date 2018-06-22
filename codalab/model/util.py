"""
Some utility classes and methods used with the CodaLab bundle model.
"""

import logging
from sqlalchemy import exc

logger = logging.getLogger(__name__)


class LikeQuery(str):
    """
    Used for a string that should be used to construct a LIKE clause instead of
    an equality clause in make_bundle_clause.
    """


def retry_if_deadlock(f):
    """
    Decorator that retries a db transaction if the transaction fails
    due to a deadlock.
    """
    lock_messages = ['Deadlock found', 'Lock wait timeout exceeded']
    DEADLOCK_MAX_ATTEMPTS = 10

    def wrapper(*args, **kwargs):
        num_attempts = 0
        while num_attempts < DEADLOCK_MAX_ATTEMPTS:
            num_attempts += 1
            try:
                return f(*args, **kwargs)
            except exc.OperationalError as e:
                if any(msg in e.message for msg in lock_messages) \
                        and num_attempts < DEADLOCK_MAX_ATTEMPTS:
                    logger.debug('Db deadlock on attempt \#%d, retrying' % num_attempts)
                else:
                    raise
    return wrapper


@retry_if_deadlock
def retrying_execute(connection, stmt, *args, **kwargs):
    """
    Wrapper for sqlalchemy engines' connection.execute that takes in a connection
    and arguments for an execute call and then retries in case of DB deadlocks
    """
    return connection.execute(stmt, *args, **kwargs)

#!./venv/bin/python
"""
This script migrates users from the Django user models to the bundle service
database.

1. Load user data from the Django database
2. Generate a new id for each user using the uuid scheme
3. Update existing rows in the bundle service users table, using the Django
   user models as a source of truth, leaving only the disk/time quota and usage
   columns intact.
4. Insert remaining users into the bundle service users table, filling in the
   configured default quota information.
5. Mark all users in the Django database as inactive, which means that they
   will not be picked up in future runs of the migration script.

By default, this script runs in dry-run mode, i.e. it prints verbose output
such as the generated SQL queries but does not make changes to the databases.
When you're ready to perform the migration, run with the '-f' flag.

This scripts assumes that you are running it from the codalab-cli directory.
"""

import os
import sys
sys.path.append('.')

from sqlalchemy import (
    select,
    bindparam,
)

from codalab.common import UsageError
from codalab.model.tables import user as cl_user
from codalab.lib.codalab_manager import (
    CodaLabManager,
    read_json_or_die,
    print_block
)


class DryRunAbort(Exception):
    """Raised at end of transaction of dry run."""
    def __str__(self):
        return """
        This was a dry run, no migration occurred. To perform full migration,
        run again with `-f':

            %s -f
        """.rstrip() % sys.argv[0]


dry_run = False if len(sys.argv) > 1 and sys.argv[1] == '-f' else True

manager = CodaLabManager()
model = manager.model()
CODALAB_HOME = manager.codalab_home

# Turn on query logging
model.engine.echo = True


###############################################################
# Configure connection to Django database
###############################################################
django_config = read_json_or_die(os.path.join(CODALAB_HOME,
                                              'website-config.json'))

# Use default settings as defined in codalab-worksheets
if 'database' not in django_config:
    django_config['database'] = {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'codalab.sqlite3',
    }

# Create database connections
if django_config['database']['ENGINE'] == 'django.db.backends.mysql':
    import MySQLdb
    from MySQLdb.cursors import DictCursor
    db_params = {
        'db': django_config['database']['NAME'],
        'user': django_config['database']['USER'],
        'passwd': django_config['database']['PASSWORD'],
        'cursorclass': DictCursor,
    }
    if django_config['database'].get('HOST', None):
        db_params['host'] = django_config['database']['HOST']
    if django_config['database'].get('PORT', None):
        db_params['port'] = django_config['database']['PORT']
    django_db = MySQLdb.connect(**db_params)
elif django_config['database']['ENGINE'] == 'django.db.backends.sqlite3':
    import sqlite3

    # Use basic dict factory, not sqlite3.Row, for mutability
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    # Default location: ../codalab-worksheets/codalab/$NAME
    sqlite_path = os.path.join(os.path.dirname(os.getcwd()),
                               'codalab-worksheets',
                               'codalab',
                               django_config['database']['NAME'])
    django_db = sqlite3.connect(sqlite_path)
    django_db.row_factory = dict_factory
else:
    raise UsageError("Invalid database engine %r" %
                     django_config['database']['ENGINE'])


###############################################################
# Begin Transaction
###############################################################

with model.engine.begin() as bundle_db, django_db as django_cursor:
    ###############################################################
    # Get existing users from both databases
    ###############################################################
    # General SQL query should work on both MySQL and sqlite3
    django_cursor.execute("""
        SELECT
        cluser.id as b_user_id,  -- prevent clash with user_id column on update
        cluser.password,
        cluser.last_login,
        cluser.username AS user_name,
        cluser.first_name,
        cluser.last_name,
        cluser.email,
        cluser.date_joined,
        cluser.organization_or_affiliation AS affiliation,
        cluser.is_superuser,
        cluser.is_active,
        email.verified AS is_verified
        FROM authenz_cluser AS cluser
        LEFT OUTER JOIN account_emailaddress AS email
        ON cluser.id = email.user_id
        WHERE cluser.id != -1 AND cluser.is_active = 1""")
    django_users = list(django_cursor.fetchall())

    # Get set of user ids in bundles db
    bundle_users = bundle_db.execute(select([cl_user.c.user_id])).fetchall()

    # Find intersection of user ids
    django_user_ids = set(str(user['b_user_id']) for user in django_users)
    bundle_user_ids = set(user['user_id'] for user in bundle_users)
    to_update = django_user_ids & bundle_user_ids
    to_insert = django_user_ids - bundle_user_ids

    print "Users to update:", ', '.join(list(to_update))
    print "Users to insert:", ', '.join(list(to_insert))

    to_update = [user for user in django_users
                 if (str(user['b_user_id']) in to_update)]
    to_insert = [user for user in django_users
                 if (str(user['b_user_id']) in to_insert)]

    ###############################################################
    # Update existing users in bundles db
    ###############################################################

    if to_update:
        print "Updating existing users in bundle service database..."

        update_query = cl_user.update().\
            where(cl_user.c.user_id == bindparam('b_user_id'))
        bundle_db.execute(update_query, to_update)

    ###############################################################
    # Insert remaining users into bundles db
    ###############################################################

    if to_insert:
        print "Inserting new users into bundle service database..."

        default_user_info = manager.default_user_info()

        insert_query = cl_user.insert().\
            values(time_quota=default_user_info['time_quota'],
                   disk_quota=default_user_info['disk_quota'],
                   time_used=0,
                   disk_used=0,
                   user_id=bindparam('b_user_id'))

        bundle_db.execute(insert_query, to_insert)

    ###############################################################
    # Deactivate users in django db
    ###############################################################

    if to_insert or to_update:
        print "Deactivating users in Django database..."

        deactivate_query = """
            UPDATE authenz_cluser
            SET is_active=0
            WHERE id IN (%s)""" % (', '.join(django_user_ids))
        print_block(deactivate_query)

        django_cursor.execute(deactivate_query)

    if dry_run:
        raise DryRunAbort

###############################################################
# Last words
###############################################################

print "Migration complete!"

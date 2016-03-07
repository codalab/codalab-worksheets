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
such as the generate SQL queries but does not make changes to the databases.
When you're ready to perform the migration, run with the '-f' flag.

This scripts assumes that you are running it from the codalab-cli directory.
"""

import os
import uuid
import sys
sys.path.append('.')

from sqlalchemy import (
    select,
    bindparam,
)

from codalab.lib.codalab_manager import CodaLabManager, read_json_or_die
from codalab.model.tables import user as cl_user


class DryRunAbort(Exception):
    """Raised at end of transaction of dry run."""
    pass


dry_run = False if len(sys.argv) > 1 and sys.argv[1] == '-f' else True

manager = CodaLabManager()
model = manager.model()

CODALAB_HOME = manager.codalab_home

###############################################################
# Load user data from Django table
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
    db = MySQLdb.connect(**db_params)
    c = db.cursor()
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
    db = sqlite3.connect(sqlite_path)
    db.row_factory = dict_factory
    c = db.cursor()

# General SQL query should work on both MySQL and sqlite3
c.execute("""
    SELECT
    cluser.id,
    cluser.password,
    cluser.last_login,
    cluser.username AS user_name,
    cluser.first_name,
    cluser.last_name,
    cluser.email,
    cluser.date_joined,
    cluser.organization_or_affiliation AS affiliation,
    cluser.is_superuser,
    email.verified AS is_verified
    FROM authenz_cluser AS cluser
    LEFT OUTER JOIN account_emailaddress AS email
    ON cluser.id = email.user_id
    WHERE cluser.id != -1 AND cluser.is_active = 1""")
django_users = list(c.fetchall())

###############################################################
# Preprocess user data
###############################################################

# Generate new user ids using uuid
for user in django_users:
    user['old_user_id'] = str(user.pop('id'))
    user['new_user_id'] = uuid.uuid4().hex

###############################################################
# Get existing users in bundles db
###############################################################

# Get set of user ids in bundles db
with model.engine.begin() as connection:
    bundle_users = connection.execute(select([cl_user.c.user_id])).fetchall()

# Find intersection of user ids
django_user_ids = set(user['old_user_id'] for user in django_users)
bundle_user_ids = set(user['user_id'] for user in bundle_users)
to_update = django_user_ids & bundle_user_ids
to_insert = django_user_ids - bundle_user_ids

print "Users to update:", ', '.join(list(to_update))
print "Users to insert:", ', '.join(list(to_insert))

to_update = [user for user in django_users
             if (user['old_user_id'] in to_update)]
to_insert = [user for user in django_users
             if (user['old_user_id'] in to_insert)]

###############################################################
# Update existing users in bundles db
###############################################################

# Turn on query logging
model.engine.echo = True

if to_update:
    try:
        with model.engine.begin() as connection:
            update_query = cl_user.update().\
                where(cl_user.c.user_id == bindparam('old_user_id')).\
                values(is_active=1, user_id=bindparam('new_user_id'))
            connection.execute(update_query, to_update)

            if dry_run:
                raise DryRunAbort
    except DryRunAbort:
        pass

###############################################################
# Insert remaining users into bundles db
###############################################################

if to_insert:
    default_user_info = manager.default_user_info()

    # Throw away old user ids
    for user in to_insert:
        del user['old_user_id']

    try:
        with model.engine.begin() as connection:
            insert_query = cl_user.insert().\
                values(user_id=bindparam('new_user_id'),
                       time_quota=default_user_info['time_quota'],
                       disk_quota=default_user_info['disk_quota'],
                       time_used=0,
                       disk_used=0)

            connection.execute(insert_query, to_insert)

            if dry_run:
                raise DryRunAbort
    except DryRunAbort:
        pass

###############################################################
# Deactivate users in django db
###############################################################

if to_insert or to_update:
    deactivate_query = """
        UPDATE authenz_cluser
        SET is_active=0
        WHERE id IN (%s)""" % (', '.join(django_user_ids))

    print deactivate_query
    if not dry_run:
        c.execute(deactivate_query)

###############################################################
# Last words
###############################################################

dry_run_str = """
This was a dry run, no migration occurred. To perform full migration,
run again with `-f':

    %s -f
""".rstrip() % sys.argv[0]

explain_str = "Migration complete!"

print >> sys.stderr, dry_run_str if dry_run else explain_str

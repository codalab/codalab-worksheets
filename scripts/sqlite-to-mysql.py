#!/usr/bin/env python

import fileinput

'''
If you already have data in SQLite, you can load it into MySQL using this script.

    sqlite3 ~/.codalab/bundle.db .dump > bundles.sqlite
    python scripts/sqlite_to_mysql.py < bundles.sqlite > bundles.mysql
    mysql -u codalab -p codalab_bundles < bundles.mysql

Once you set up your database, run the following so that future migrations
start from the right place (this is important!):

    venv/bin/alembic stamp head
'''

# Adapted from http://paulasmuth.com/blog/migrate_sqlite_to_mysql/
# Reads from stdin (output of sqlite3 <db> .dump), writes to stdout.

# Suppress errors when record contains invalid foreign key.  This is here
# because sqlite is less strict than mysql, but avoid doing this.
#print 'SET FOREIGN_KEY_CHECKS=0;'

for line in fileinput.input():
    line = line.strip()
    line = line.replace("\"", "`").replace("\\''", "\\'")
    line = line.replace("AUTOINCREMENT", "AUTO_INCREMENT")

    # Skip stuff
    if line == 'PRAGMA foreign_keys=OFF;': continue
    if line == 'BEGIN TRANSACTION;': continue
    if line == 'COMMIT;': continue
    if line == 'DELETE FROM sqlite_sequence;': continue
    if line.startswith('INSERT INTO `sqlite_sequence`'): continue

    # http://stackoverflow.com/questions/1827063/mysql-error-key-specification-without-a-key-length
    # The sqlite dump doesn't put a maximum character limit on indexes for text fields.
    # Hack: hard code it.
    if line.startswith('CREATE INDEX'):
        line = line.replace('metadata_value', 'metadata_value(255)')
    print line

#print 'SET FOREIGN_KEY_CHECKS=1;'

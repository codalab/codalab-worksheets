#!/usr/bin/env python

import fileinput

# Used to migrate sqlite to mysql.

# Adapted from http://paulasmuth.com/blog/migrate_sqlite_to_mysql/
# Reads from stdin (output of sqlite3 <db> .dump), writes to stdout.

# Suppress errors when record contains invalid foreign key.
print 'SET FOREIGN_KEY_CHECKS=0;'

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

print 'SET FOREIGN_KEY_CHECKS=1;'

'''
EnvModel is a lightweight model for storing variables that are persisted
across multiple command-line client invocations in the same shell. In
particular, we do NOT import sqlalchemy in this module, because that library
is quite expensive to import.
'''
# TODO(skishore): If we want to support work contexts that are indexed by
# keys other than the shell ppid, this model's get / set methods will just have
# to take additional parameters for the key.
import os
import sqlite3


class EnvModel(object):
    SQLITE_DB_FILE_NAME = 'env.db'

    def __init__(self, home):
        sqlite_db_path = os.path.join(home, self.SQLITE_DB_FILE_NAME)
        self.connection = sqlite3.connect(sqlite_db_path)
        self.create_tables()

    def create_tables(self):
        # Create a table mapping shell process ids to worksheets.
        self.connection.execute('''
          CREATE TABLE IF NOT EXISTS worksheets (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            ppid INTEGER NOT NULL,
            worksheet_uuid VARCHAR(63) NOT NULL,
            CONSTRAINT uix_1 UNIQUE(ppid)
          );
        ''')

    def get_current_worksheet(self):
        '''
        Return a worksheet_uuid for the current worksheet, or None if there is none.

        This method uses the current parent-process id to return the same result
        across multiple invocations in the same shell.
        '''
        ppid = os.getppid()
        cursor = self.connection.cursor()
        cursor.execute('SELECT * FROM worksheets WHERE ppid = ?;', (ppid,))
        row = cursor.fetchone()
        return row[2] if row else None

    def set_current_worksheet(self, worksheet_uuid):
        '''
        Set the current worksheet for this ppid.
        '''
        ppid = os.getppid()
        with self.connection:
            self.connection.execute('''
              INSERT OR REPLACE INTO worksheets (ppid, worksheet_uuid) VALUES (?, ?);
            ''', (ppid, worksheet_uuid))

    def clear_current_worksheet(self):
        '''
        Clear the current worksheet setting for this ppid.
        '''
        ppid = os.getppid()
        with self.connection:
            self.connection.execute('DELETE FROM worksheets WHERE ppid = ?;', (ppid,))

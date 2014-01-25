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
        worksheet_spec VARCHAR(255) NOT NULL,
        CONSTRAINT uix_1 UNIQUE(ppid)
      );
    ''')

  def get_current_worksheet(self):
    '''
    Return a (worksheet_uuid, worksheet_spec) pair representing the
    current worksheet, or (None, None) if there is none.

    This method uses the current parent-process id to return the same
    result across multiple invocations in the same shell.
    '''
    ppid = os.getppid()
    cursor = self.connection.cursor()
    cursor.execute('SELECT * FROM worksheets WHERE ppid = ?;', (ppid,))
    row = cursor.fetchone()
    if row:
      return (row[2], row[3])
    return (None, None)

  def set_current_worksheet(self, worksheet_uuid, worksheet_spec):
    '''
    Set the current worksheet for this ppid.
    '''
    ppid = os.getppid()
    with self.connection:
      self.connection.execute('''
        INSERT OR REPLACE INTO worksheets
          (ppid, worksheet_uuid, worksheet_spec)
          VALUES (?, ?, ?);
      ''', (ppid, worksheet_uuid, worksheet_spec))

  def clear_current_worksheet(self):
    '''
    Clear the current worksheet setting for this ppid.
    '''
    ppid = os.getppid()
    with self.connection:
      self.connection.execute('DELETE FROM worksheets WHERE ppid = ?;', (ppid,))

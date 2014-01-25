'''
SQLiteModel is a subclass of BundleModel that stores metadata in a sqlite3
database in a local file in the CodaLab home directory.
'''
import os
from sqlalchemy import create_engine

from codalab.model.bundle_model import BundleModel


class SQLiteModel(BundleModel):
  SQLITE_DB_FILE_NAME = 'bundle.db'

  def __init__(self, home):
    sqlite_db_path = os.path.join(home, self.SQLITE_DB_FILE_NAME)
    engine_url = 'sqlite:///%s' % (sqlite_db_path,)
    engine = create_engine(engine_url, strategy='threadlocal')
    super(SQLiteModel, self).__init__(engine)

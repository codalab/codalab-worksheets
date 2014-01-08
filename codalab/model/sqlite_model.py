'''
SQLiteModel is a subclass of BundleModel which stores the metadata in a
sqlite3 database in a local file in the CodaLab home directory.
'''
import os
from sqlalchemy import create_engine

from codalab.lib import path_util
from codalab.model.bundle_model import BundleModel


class SQLiteModel(BundleModel):
  SQLITE_DB_FILE_NAME = 'bundle.db'

  def __init__(self, codalab_home):
    path_util.check_isdir(codalab_home, 'SQLiteModel.__init__')
    sqlite_db_path = os.path.join(codalab_home, self.SQLITE_DB_FILE_NAME)
    engine_url = 'sqlite:///%s' % (sqlite_db_path,)
    engine = create_engine(engine_url, strategy='threadlocal')
    super(SQLiteModel, self).__init__(engine)

'''
SQLiteModel is a subclass of BundleModel which stores the metadata in a
sqlite3 database in a local file in the CodaLab home directory.
'''
from sqlalchemy import create_engine

from codalab.model.bundle_model import BundleModel


class MySQLModel(BundleModel):
  def __init__(self, username, password, address, database):
    engine_url = 'mysql://%s%s@%s/%s' % (
      username,
      (':' + password if password else ''),
      address,
      database,
    )
    engine = create_engine(engine_url, strategy='threadlocal')
    super(MySQLModel, self).__init__(engine)

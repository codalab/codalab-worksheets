import os
import sqlite3

from codalab.bundle_model import BundleModel


class LocalBundleClient(object):
  BUNDLES_DB_NAME = 'bundle.db'

  def __init__(self, home):
    # CodaLab data structures will live in a home directory that this client
    # needs to know the location of. This directory defaults to $HOME/.codalab.
    self.home = home
    bundles_db_path = os.path.join(self.home, self.BUNDLES_DB_NAME)
    self.bundles_db = sqlite3.connect(bundles_db_path)
    self.cursor = self.bundles_db.cursor()
    self.model = BundleModel(self.cursor)

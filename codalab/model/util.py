'''
Provides a method to get a BundleModel object. This method should read model
configuration parameters (such as database types, connection parameters, etc)
from a file. It does not do that yet.
'''
from codalab.model.sqlite_model import SQLiteModel


_models = {}


def get_codalab_model(codalab_home):
  if codalab_home not in _models:
    _models[codalab_home] = SQLiteModel(codalab_home)
    _models[codalab_home].create_tables()
  return _models[codalab_home]

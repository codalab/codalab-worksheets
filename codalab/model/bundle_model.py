from sqlalchemy import (
  and_,
  select,
)

from codalab.bundles import get_bundle_subclass
from codalab.model.tables import (
  bundle as cl_bundle,
  bundle_metadata as cl_bundle_metadata,
  db_metadata,
)


class BundleModel(object):
  def __init__(self, engine):
    '''
    Initialize a BundleModel with the given SQLAlchemy engine.
    '''
    self.engine = engine

  def _reset(self):
    '''
    Do a drop / create table to clear and reset the schema of all tables.
    '''
    # Do not run this function in production!
    db_metadata.drop_all(self.engine)
    self.create_tables()

  def create_tables(self):
    '''
    Create all Codalab bundle tables if they do not already exist.
    '''
    db_metadata.create_all(self.engine)

  def get_bundle(self, uuid):
    '''
    Retrieve a bundle from the database given its uuid.
    '''
    bundles = self.batch_get_bundles(uuids=[uuid])
    if not bundles:
      raise ValueError('Could not find bundle with uuid %s' % (uuid,))
    elif len(bundles) > 1:
      raise ValueError('Found multiple bundles with uuid %s' % (uuid,))
    return bundles[0]

  def search_bundles(self, **kwargs):
    '''
    Returns a list of bundles that match the given metadata search.
    '''
    if len(kwargs) != 1:
      raise NotImplementedError('Complex searches have not been implemented.')
    [(key, value)] = kwargs.items()
    clause = and_(
      cl_bundle_metadata.c.metadata_key == key,
      cl_bundle_metadata.c.metadata_value == value,
    )
    with self.engine.begin() as connection:
      metadata_rows = connection.execute(select([
        cl_bundle_metadata.c.bundle_uuid,
      ]).where(clause)).fetchall()
    uuids = set([row.bundle_uuid for row in metadata_rows])
    if not uuids:
      return []
    return self.batch_get_bundles(uuids)

  def batch_get_bundles(self, uuids):
    '''
    Return a list of bundles given their uuids.
    '''
    with self.engine.begin() as connection:
      bundle_rows = connection.execute(cl_bundle.select().where(
        cl_bundle.c.uuid.in_(uuids)
      )).fetchall()
      metadata_rows = connection.execute(cl_bundle_metadata.select().where(
        cl_bundle_metadata.c.bundle_uuid.in_(uuids)
      )).fetchall()
    # Make a dictionary for each bundle with both data and metadata.
    bundle_values = {row.uuid: dict(row) for row in bundle_rows}
    for bundle_value in bundle_values.itervalues():
      bundle_value['metadata'] = []
    for metadata_row in metadata_rows:
      if metadata_row.bundle_uuid not in bundle_values:
        raise ValueError('Got metadata %s for deleted bundle' % (metadata_row,))
      bundle_values[metadata_row.bundle_uuid]['metadata'].append(metadata_row)
    # Construct and validate all of the retrieved bundles.
    bundles = [
      get_bundle_subclass(bundle_value['bundle_type'])(bundle_value)
      for bundle_value in bundle_values.itervalues()
    ]
    for bundle in bundles:
      bundle.validate()
    return bundles

  def save_bundle(self, bundle):
    '''
    Save a bundle. On success, sets the Bundle object's id from the result.
    '''
    bundle.validate()
    bundle_value = bundle.to_dict()
    bundle_metadata_values = bundle_value.pop('metadata')
    with self.engine.begin() as connection:
      result = connection.execute(cl_bundle.insert().values(bundle_value))
      connection.execute(cl_bundle_metadata.insert(), bundle_metadata_values)
      bundle.id = result.lastrowid

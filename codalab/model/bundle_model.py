from sqlalchemy import (
  and_,
  select,
)

from codalab.bundles import get_bundle_subclass
from codalab.common import (
  IntegrityError,
  precondition,
  UsageError,
)
from codalab.model.tables import (
  bundle as cl_bundle,
  bundle_metadata as cl_bundle_metadata,
  dependency as cl_dependency,
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

  def do_multirow_insert(self, connection, table, values):
    '''
    Insert multiple rows into the given table.

    This method may be overridden by models that use more powerful SQL dialects.
    '''
    # This is a lowest-common-denominator implementation of a multi-row insert.
    # It deals with a couple of SQL dialect issues:
    #   - Some dialects do not support empty inserts, so we test 'if values'.
    #   - Some dialects do not support multiple inserts in a single statement,
    #     which we deal with by using the DBAPI execute_many pattern.
    if values:
      connection.execute(table.insert(), values)

  def make_bundle_clause(self, kwargs):
    '''
    Return a list of bundles given a dict mapping cl_bundle columns to values.
    If a value is a list, set, or tuple, produce an IN clause on that column.
    '''
    clauses = [True]
    for (key, value) in kwargs.iteritems():
      if isinstance(value, (list, set, tuple)):
        if not value:
          return False
        clauses.append(getattr(cl_bundle.c, key).in_(value))
      else:
        clauses.append(getattr(cl_bundle.c, key) == value)
    return and_(*clauses)

  def get_bundle(self, uuid):
    '''
    Retrieve a bundle from the database given its uuid.
    '''
    bundles = self.batch_get_bundles(uuid=uuid)
    if not bundles:
      raise UsageError('Could not find bundle with uuid %s' % (uuid,))
    elif len(bundles) > 1:
      raise IntegrityError('Found multiple bundles with uuid %s' % (uuid,))
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
    return self.batch_get_bundles(uuid=uuids)

  def batch_get_bundles(self, **kwargs):
    '''
    Return a list of bundles given a SQLAlchemy clause on the cl_bundle table.
    '''
    clause = self.make_bundle_clause(kwargs)
    with self.engine.begin() as connection:
      bundle_rows = connection.execute(
        cl_bundle.select().where(clause)
      ).fetchall()
      uuids = set(bundle_row.uuid for bundle_row in bundle_rows)
      if not uuids:
        return []
      metadata_rows = connection.execute(cl_bundle_metadata.select().where(
        cl_bundle_metadata.c.bundle_uuid.in_(uuids)
      )).fetchall()
      dependency_rows = connection.execute(cl_dependency.select().where(
        cl_dependency.c.child_uuid.in_(uuids)
      )).fetchall()
    # Make a dictionary for each bundle with both data and metadata.
    bundle_values = {row.uuid: dict(row) for row in bundle_rows}
    for bundle_value in bundle_values.itervalues():
      bundle_value['metadata'] = []
      bundle_value['dependencies'] = []
    for metadata_row in metadata_rows:
      if metadata_row.bundle_uuid not in bundle_values:
        raise IntegrityError('Got metadata %s without bundle' % (metadata_row,))
      bundle_values[metadata_row.bundle_uuid]['metadata'].append(metadata_row)
    for dep_row in dependency_rows:
      if dep_row.child_uuid not in bundle_values:
        raise IntegrityError('Got dependency %s without bundle' % (dep_row,))
      bundle_values[dep_row.child_uuid]['dependencies'].append(dep_row)
    # Construct and validate all of the retrieved bundles.
    sorted_values = sorted(bundle_values.itervalues(), key=lambda r: r['id'])
    bundles = [
      get_bundle_subclass(bundle_value['bundle_type'])(bundle_value)
      for bundle_value in sorted_values
    ]
    for bundle in bundles:
      bundle.validate()
    return bundles

  def batch_update_bundles(self, bundles, update, condition=None):
    '''
    Update a list of bundles given a dict mapping columns to new values.
    Return True if all updates succeed.
    '''
    precondition('id' not in update, 'Illegal update: %s' % (update,))
    if bundles:
      bundle_ids = set(bundle.id for bundle in bundles)
      clause = cl_bundle.c.id.in_(bundle_ids)
      if condition:
        clause = and_(clause, self.make_bundle_clause(condition))
      with self.engine.begin() as connection:
        result = connection.execute(
          cl_bundle.update().where(clause).values(update)
        )
        success = result.rowcount == len(bundle_ids)
        if success:
          for bundle in bundles:
            bundle.update_in_memory(update)
        return success
    return True

  def save_bundle(self, bundle):
    '''
    Save a bundle. On success, sets the Bundle object's id from the result.
    '''
    bundle.validate()
    bundle_value = bundle.to_dict()
    metadata_values = bundle_value.pop('metadata')
    dependency_values = bundle_value.pop('dependencies')
    with self.engine.begin() as connection:
      result = connection.execute(cl_bundle.insert().values(bundle_value))
      self.do_multirow_insert(connection, cl_bundle_metadata, metadata_values)
      self.do_multirow_insert(connection, cl_dependency, dependency_values)
      bundle.id = result.lastrowid

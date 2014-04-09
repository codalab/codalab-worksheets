'''
BundleModel is a wrapper around database calls to save and load bundle metadata.
'''
from sqlalchemy import (
  and_,
  select,
)
from sqlalchemy.exc import (
  OperationalError,
  ProgrammingError,
)
from sqlalchemy.sql.expression import true

from codalab.bundles import get_bundle_subclass
from codalab.common import (
  IntegrityError,
  precondition,
  UsageError,
)
from codalab.model.util import LikeQuery
from codalab.model.tables import (
  bundle as cl_bundle,
  bundle_dependency as cl_bundle_dependency,
  bundle_metadata as cl_bundle_metadata,
  worksheet as cl_worksheet,
  worksheet_item as cl_worksheet_item,
  db_metadata,
)
from codalab.objects.worksheet import (
  item_sort_key,
  Worksheet,
)


class BundleModel(object):
    def __init__(self, engine):
        '''
        Initialize a BundleModel with the given SQLAlchemy engine.
        '''
        self.engine = engine
        self.create_tables()

    def _reset(self):
        '''
        Do a drop / create table to clear and reset the schema of all tables.
        '''
        # Do not run this function in production!
        db_metadata.drop_all(self.engine)
        self.create_tables()

    def create_tables(self):
        '''
        Create all CodaLab bundle tables if they do not already exist.
        '''
        # TODO(skishore): This hack is a mini-migration that should stay here until
        # the bundle dependency table has been renamed in all CodaLab deployments.
        # After that point, it should be deleted.
        try:
            with self.engine.begin() as connection:
                connection.execute('ALTER TABLE dependency RENAME TO bundle_dependency')
        except (OperationalError, ProgrammingError):
            # sqlite throws an OperationalError, MySQL a ProgrammingError. Ugh.
            pass
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

    def make_kwargs_clause(self, table, kwargs):
        '''
        Return a list of bundles given a dict mapping table columns to values.
        If a value is a list, set, or tuple, produce an IN clause on that column.
        If a value is a LikeQuery, produce a LIKE clause on that column.
        '''
        clauses = [true()]
        for (key, value) in kwargs.iteritems():
            if isinstance(value, (list, set, tuple)):
                if not value:
                    return False
                clauses.append(getattr(table.c, key).in_(value))
            elif isinstance(value, LikeQuery):
                clauses.append(getattr(table.c, key).like(value))
            else:
                clauses.append(getattr(table.c, key) == value)
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

    def get_parents(self, uuid):
        '''
        Get all bundles that the bundle with the given uuid depends on.
        '''
        with self.engine.begin() as connection:
            rows = connection.execute(select([
              cl_bundle_dependency.c.parent_uuid
            ]).where(
              cl_bundle_dependency.c.child_uuid == uuid
            )).fetchall()
        uuids = set([row.parent_uuid for row in rows])
        return self.batch_get_bundles(uuid=uuids)

    def get_children(self, uuid):
        '''
        Get all bundles that depend on the bundle with the given uuid.

        uuid may also be a list, set, or tuple, in which case we return all bundles
        that depend on any bundle in that collection. This mode is used to optimize
        calls to delete_bundle_tree.
        '''
        if isinstance(uuid, (list, set, tuple)):
            clause = cl_bundle_dependency.c.parent_uuid.in_(uuid)
        else:
            clause = (cl_bundle_dependency.c.parent_uuid == uuid)
        with self.engine.begin() as connection:
            rows = connection.execute(select([
              cl_bundle_dependency.c.child_uuid
            ]).where(clause)).fetchall()
        uuids = set([row.child_uuid for row in rows])
        return self.batch_get_bundles(uuid=uuids)

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
        clause = self.make_kwargs_clause(cl_bundle, kwargs)
        with self.engine.begin() as connection:
            bundle_rows = connection.execute(
              cl_bundle.select().where(clause)
            ).fetchall()
            if not bundle_rows:
                return []
            uuids = set(bundle_row.uuid for bundle_row in bundle_rows)
            dependency_rows = connection.execute(cl_bundle_dependency.select().where(
              cl_bundle_dependency.c.child_uuid.in_(uuids)
            )).fetchall()
            metadata_rows = connection.execute(cl_bundle_metadata.select().where(
              cl_bundle_metadata.c.bundle_uuid.in_(uuids)
            )).fetchall()
        # Make a dictionary for each bundle with both data and metadata.
        bundle_values = {row.uuid: dict(row) for row in bundle_rows}
        for bundle_value in bundle_values.itervalues():
            bundle_value['dependencies'] = []
            bundle_value['metadata'] = []
        for dep_row in dependency_rows:
            if dep_row.child_uuid not in bundle_values:
                raise IntegrityError('Got dependency %s without bundle' % (dep_row,))
            bundle_values[dep_row.child_uuid]['dependencies'].append(dep_row)
        for metadata_row in metadata_rows:
            if metadata_row.bundle_uuid not in bundle_values:
                raise IntegrityError('Got metadata %s without bundle' % (metadata_row,))
            bundle_values[metadata_row.bundle_uuid]['metadata'].append(metadata_row)
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
        Update a list of bundles given a dict mapping columns to new values and
        return True if all updates succeed. This method does NOT update metadata.

        If a condition is specified, only update bundles that satisfy the condition.

        In general, this method should only be used for programmatic updates, as in
        the bundle worker. It is provided as an efficient way to perform a simple
        update on many, but these updates are not validated.
        '''
        message = 'Illegal update: %s' % (update,)
        precondition('id' not in update and 'uuid' not in update, message)
        if bundles:
            bundle_ids = set(bundle.id for bundle in bundles)
            clause = cl_bundle.c.id.in_(bundle_ids)
            if condition:
                clause = and_(clause, self.make_kwargs_clause(cl_bundle, condition))
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
        dependency_values = bundle_value.pop('dependencies')
        metadata_values = bundle_value.pop('metadata')
        with self.engine.begin() as connection:
            result = connection.execute(cl_bundle.insert().values(bundle_value))
            self.do_multirow_insert(connection, cl_bundle_dependency, dependency_values)
            self.do_multirow_insert(connection, cl_bundle_metadata, metadata_values)
            bundle.id = result.lastrowid

    def update_bundle(self, bundle, update):
        '''
        Update a bundle's columns and metadata in the database and in memory.
        The update is done as a diff: columns that do not appear in the update dict
        and metadata keys that do not appear in the metadata sub-dict are unaffected.

        This method validates all updates to the bundle, so it is appropriate
        to use this method to update bundles based on user input (eg: cl edit).
        '''
        message = 'Illegal update: %s' % (update,)
        precondition('id' not in update and 'uuid' not in update, message)
        # Apply the column and metadata updates in memory and validate the result.
        metadata_update = update.pop('metadata', {})
        bundle.update_in_memory(update)
        for (key, value) in metadata_update.iteritems():
            bundle.metadata.set_metadata_key(key, value)
        bundle.validate()
        # Construct clauses and update lists for updating certain bundle columns.
        if update:
            clause = cl_bundle.c.uuid == bundle.uuid
        if metadata_update:
            metadata_clause = and_(
              cl_bundle_metadata.c.bundle_uuid == bundle.uuid,
              cl_bundle_metadata.c.metadata_key.in_(metadata_update)
            )
            metadata_values = [
              row_dict for row_dict in bundle.to_dict().pop('metadata')
              if row_dict['metadata_key'] in metadata_update
            ]
        # Perform the actual updates.
        with self.engine.begin() as connection:
            if update:
                connection.execute(cl_bundle.update().where(clause).values(update))
            if metadata_update:
                connection.execute(cl_bundle_metadata.delete().where(metadata_clause))
                self.do_multirow_insert(connection, cl_bundle_metadata, metadata_values)

    def delete_bundle_tree(self, uuids, force=False):
        '''
        Delete bundles with the given uuids and all bundles that are (direct or
        indirect) descendents of them.

        If force is False, there should be no descendents of the given bundles.
        '''
        children = self.get_children(uuid=uuids)
        if children:
            precondition(force, 'Bundles depend on %s:\n  %s' % (
              self.get_bundle(uuids[0]),
              '\n  '.join(str(child) for child in children),
            ))
            self.delete_bundle_tree([child.uuid for child in children], force=True)
        with self.engine.begin() as connection:
            # We must delete bundles rows in the opposite order that we create them
            # to avoid foreign-key constraint failures.
            connection.execute(cl_bundle_metadata.delete().where(
              cl_bundle_metadata.c.bundle_uuid.in_(uuids)
            ))
            connection.execute(cl_bundle_dependency.delete().where(
              cl_bundle_dependency.c.child_uuid.in_(uuids)
            ))
            connection.execute(cl_bundle.delete().where(cl_bundle.c.uuid.in_(uuids)))

    #############################################################################
    # Worksheet-related model methods follow!
    #############################################################################

    def get_worksheet(self, uuid):
        '''
        Get a worksheet given its uuid.
        '''
        worksheets = self.batch_get_worksheets(uuid=uuid)
        if not worksheets:
            raise UsageError('Could not find worksheet with uuid %s' % (uuid,))
        elif len(worksheets) > 1:
            raise IntegrityError('Found multiple workseets with uuid %s' % (uuid,))
        return worksheets[0]

    def get_child_worksheets(self, bundle_uuid):
        '''
        Return a list of worksheets that depend on the given bundle.
        '''
        with self.engine.begin() as connection:
            rows = connection.execute(cl_worksheet_item.select().where(
              cl_worksheet_item.c.bundle_uuid == bundle_uuid
            )).fetchall()
        uuids = set(row.worksheet_uuid for row in rows)
        return self.batch_get_worksheets(uuid=uuids)

    def batch_get_worksheets(self, **kwargs):
        '''
        Get a list of worksheets, all of which satisfy the clause given by kwargs.
        '''
        clause = self.make_kwargs_clause(cl_worksheet, kwargs)
        with self.engine.begin() as connection:
            worksheet_rows = connection.execute(
              cl_worksheet.select().where(clause)
            ).fetchall()
            if not worksheet_rows:
                return []
            uuids = set(row.uuid for row in worksheet_rows)
            item_rows = connection.execute(cl_worksheet_item.select().where(
              cl_worksheet_item.c.worksheet_uuid.in_(uuids)
            )).fetchall()
        # Make a dictionary for each worksheet with both its main row and its items.
        worksheet_values = {row.uuid: dict(row) for row in worksheet_rows}
        for value in worksheet_values.itervalues():
            value['items'] = []
        for item_row in sorted(item_rows, key=item_sort_key):
            if item_row.worksheet_uuid not in worksheet_values:
                raise IntegrityError('Got item %s without worksheet' % (item_row,))
            worksheet_values[item_row.worksheet_uuid]['items'].append(item_row)
        return [Worksheet(value) for value in worksheet_values.itervalues()]

    def list_worksheets(self):
        '''
        Return a list of row dicts, one per worksheet. These dicts do NOT contain
        worksheet items; this method is meant to make it easy for a user to see
        the currently existing worksheets.
        '''
        with self.engine.begin() as connection:
            rows = connection.execute(cl_worksheet.select()).fetchall()
        return [dict(row) for row in sorted(rows, key=lambda row: row.id)]

    def save_worksheet(self, worksheet):
        '''
        Save the given (empty) worksheet to the database. On success, set its id.
        '''
        message = 'save_worksheet called with non-empty worksheet: %s' % (worksheet,)
        precondition(not worksheet.items, message)
        worksheet.validate()
        worksheet_value = worksheet.to_dict()
        with self.engine.begin() as connection:
            result = connection.execute(cl_worksheet.insert().values(worksheet_value))
            worksheet.id = result.lastrowid

    def add_worksheet_item(self, worksheet_uuid, item):
        '''
        Appends a new item to the end of the given worksheet. The item should be
        a (bundle_uuid, value, type) pair, where the bundle_uuid may be None and the
        value must be a string.
        '''
        (bundle_uuid, value, type) = item
        item_value = {
          'worksheet_uuid': worksheet_uuid,
          'bundle_uuid': bundle_uuid,
          'type': type,
          'value': value,
          'sort_key': None,
        }
        with self.engine.begin() as connection:
            connection.execute(cl_worksheet_item.insert().values(item_value))

    def update_worksheet(self, worksheet_uuid, last_item_id, length, new_items):
        '''
        Updates the worksheet with the given uuid. If there were exactly
        `last_length` items with database id less than `last_id`, replaces them all
        with the items in new_items. Does NOT affect items in this worksheet with
        database id greater than last_id.

        Does NOT affect items that were added to the worksheet in between the
        time it was retrieved and it was updated.

        If this worksheet were updated between the time it was retrieved and
        updated, this method will raise a UsageError.
        '''
        clause = and_(
          cl_worksheet_item.c.worksheet_uuid == worksheet_uuid,
          cl_worksheet_item.c.id <= last_item_id,
        )
        # See codalab.objects.worksheet for an explanation of the sort_key protocol.
        # We need to produce sort keys here that are strictly upper-bounded by the
        # last known item id in this worksheet, and which monotonically increase.
        # The expression last_item_id + i - len(new_items) works. It can produce
        # negative sort keys, but that's fine.
        new_item_values = [{
          'worksheet_uuid': worksheet_uuid,
          'bundle_uuid': bundle_uuid,
          'type': type,
          'value': value,
          'sort_key': (last_item_id + i - len(new_items)),
        } for (i, (bundle_uuid, value, type)) in enumerate(new_items)]
        with self.engine.begin() as connection:
            result = connection.execute(cl_worksheet_item.delete().where(clause))
            message = 'Found extra items for worksheet %s' % (worksheet_uuid,)
            precondition(result.rowcount <= length, message)
            if result.rowcount < length:
                raise UsageError('Worksheet %s was updated concurrently!' % (worksheet_uuid,))
            self.do_multirow_insert(connection, cl_worksheet_item, new_item_values)

    def rename_worksheet(self, worksheet, name):
        '''
        Update the given worksheet's name.
        '''
        worksheet.name = name
        worksheet.validate()
        with self.engine.begin() as connection:
            connection.execute(cl_worksheet.update().where(
              cl_worksheet.c.uuid == worksheet.uuid
            ).values({'name': name}))

    def delete_worksheet(self, worksheet_uuid):
        '''
        Delete the worksheet with the given uuid.
        '''
        with self.engine.begin() as connection:
            connection.execute(cl_worksheet_item.delete().where(
              cl_worksheet_item.c.worksheet_uuid == worksheet_uuid
            ))
            connection.execute(cl_worksheet.delete().where(
              cl_worksheet.c.uuid == worksheet_uuid
            ))

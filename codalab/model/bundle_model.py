'''
BundleModel is a wrapper around database calls to save and load bundle metadata.
'''
from sqlalchemy import (
    and_,
    or_,
    not_,
    select,
    union,
)
from sqlalchemy.exc import (
    OperationalError,
    ProgrammingError,
)
from sqlalchemy.sql.expression import (
    literal,
    true,
)

from codalab.bundles import get_bundle_subclass
from codalab.common import (
    IntegrityError,
    precondition,
    UsageError,
)
from codalab.lib import (
    spec_util,
    worksheet_util,
)
from codalab.model.util import LikeQuery
from codalab.model.tables import (
    bundle as cl_bundle,
    bundle_dependency as cl_bundle_dependency,
    bundle_metadata as cl_bundle_metadata,
    bundle_action as cl_bundle_action,
    group as cl_group,
    group_object_permission as cl_group_object_permission,
    GROUP_OBJECT_PERMISSION_ALL,
    GROUP_OBJECT_PERMISSION_READ,
    GROUP_OBJECT_PERMISSION_NONE,
    user_group as cl_user_group,
    worksheet as cl_worksheet,
    worksheet_item as cl_worksheet_item,
    db_metadata,
)
from codalab.objects.worksheet import (
    item_sort_key,
    Worksheet,
)

import re, collections

CONDITION_REGEX = re.compile('^([\w/]+)=(.*)$')

class BundleModel(object):
    def __init__(self, engine):
        '''
        Initialize a BundleModel with the given SQLAlchemy engine.
        '''
        self.engine = engine
        self.public_group_uuid = ''
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
        db_metadata.create_all(self.engine)
        self._create_default_groups()

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

    def make_clause(self, key, value):
        if isinstance(value, (list, set, tuple)):
            if not value:
                return False
            return key.in_(value)
        elif isinstance(value, LikeQuery):
            return key.like(value)
        else:
            return key == value

    def make_kwargs_clause(self, table, kwargs):
        '''
        Return a list of bundles given a dict mapping table columns to values.
        If a value is a list, set, or tuple, produce an IN clause on that column.
        If a value is a LikeQuery, produce a LIKE clause on that column.
        '''
        clauses = [true()]
        for (key, value) in kwargs.iteritems():
            clauses.append(self.make_clause(getattr(table.c, key), value))
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

    # TODO: integrate with get_bundle, but we need more custom logic for
    # selecting out parts of the bundle.
    def get_name(self, uuid):
        '''
        Return the name of the bundle with given uuid.
        '''
        with self.engine.begin() as connection:
            rows = connection.execute(select([
              cl_bundle_metadata.c.metadata_value
            ]).where(
              and_(cl_bundle_metadata.c.metadata_key == 'name',
                   cl_bundle_metadata.c.bundle_uuid == uuid)
            )).fetchall()
            if len(rows) > 1:
                raise IntegrityError('Bundle %s has more than one name: %s' % (uuid, rows))
            if len(rows) == 0:  # uuid might not be in the database (broken links are possible)
                return None
            return rows[0][0]

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

    def get_children_uuids(self, uuid):
        '''
        Get all bundles that depend on the bundle with the given uuid.
        uuid may also be a list, set, or tuple, in which case we return all
        bundles that depend on any bundle in that collection.
        '''
        if isinstance(uuid, (list, set, tuple)):
            clause = cl_bundle_dependency.c.parent_uuid.in_(uuid)
        else:
            clause = (cl_bundle_dependency.c.parent_uuid == uuid)
        with self.engine.begin() as connection:
            rows = connection.execute(select([
              cl_bundle_dependency.c.child_uuid
            ]).where(clause)).fetchall()
        return set([row.child_uuid for row in rows])

    def get_self_and_descendants(self, uuids, depth):
        '''
        Get all bundles that depend on bundles with the given uuids.
        depth = 1 gets only children
        '''
        frontier = set(uuids)
        visited = set(frontier)
        while len(frontier) > 0 and depth > 0:
            new_frontier = self.get_children_uuids(frontier)
            frontier = new_frontier - visited
            visited.update(frontier)
            depth -= 1
        return visited

    def get_bundle_uuids(self, conditions, max_results, count=False):
        '''
        Returns a list of bundle_uuids that have match the conditions.
        Possible conditions on bundles (not all supported right now):
        - uuid: could be just a prefix
        - name (exists in bundle_metadata)
        - parent: dependent (exists in bundle_dependency)
        - child: downstream influence (exists in bundle_dependency)
        - worksheet_uuid (exists in worksheet_uuid)
        - user_id (must be reachable from this user)
        Return in reverse order.
        '''
        # TODO: implement 'count'
        # TODO: handle matching other conditions
        # TODO: support user_id (to implement permissions!)
        if 'uuid' in conditions:
            # Match the uuid only
            clause = self.make_clause(cl_bundle.c.uuid, conditions['uuid'])
            query = select([cl_bundle.c.uuid]).where(clause)
        elif 'name' in conditions:
            # Select name
            if conditions['name']:
                clause = and_(
                  cl_bundle_metadata.c.metadata_key == 'name',
                  self.make_clause(cl_bundle_metadata.c.metadata_value, conditions['name'])
                )
            else:
                clause = true()
            if conditions['worksheet_uuid']:
                # Select things on the given worksheet
                clause = and_(clause, self.make_clause(cl_worksheet_item.c.worksheet_uuid, conditions['worksheet_uuid']))
                clause = and_(clause, cl_worksheet_item.c.bundle_uuid == cl_bundle_metadata.c.bundle_uuid)  # Join
                query = select([cl_bundle_metadata.c.bundle_uuid, cl_worksheet_item.c.id]).distinct().where(clause)
                query = query.order_by(cl_worksheet_item.c.id.desc()).limit(max_results)
            else:
                if not conditions['name']:
                    raise UsageError('Nothing is specified')
                # Select from all bundles
                clause = and_(clause, cl_bundle.c.uuid == cl_bundle_metadata.c.bundle_uuid)  # Join
                query = select([cl_bundle.c.uuid]).where(clause)
                query = query.order_by(cl_bundle.c.id.desc()).limit(max_results)
        elif '*' in conditions:
            # Search any field: uuid, command, other metadata
            # Each keyword is either a string (just search everywhere) or
            # key=value, which is more targeted, and results in exact match.
            clauses = []
            offset = 0

            for keyword in conditions['*']:
                m = CONDITION_REGEX.match(keyword)
                if m:
                    key, value = m.group(1), m.group(2)
                    if key == 'bundle_type' or key == 'type':
                        clause = (cl_bundle.c.bundle_type == value)
                    elif key == 'data_hash':
                        clause = (cl_bundle.c.data_hash == value)
                    elif key == 'state':
                        clause = (cl_bundle.c.state == value)
                    elif key == 'dependencies':
                        clause = and_(
                            cl_bundle_dependency.c.child_uuid == cl_bundle.c.uuid,  # Join constraint
                            cl_bundle_dependency.c.parent_uuid == value,  # Match the uuid of the dependent (parent)
                        )
                    elif key.startswith('dependencies/'):
                        _, name = key.split('/', 1)
                        clause = and_(
                            cl_bundle_dependency.c.child_uuid == cl_bundle.c.uuid,  # Join constraint
                            cl_bundle_dependency.c.parent_uuid == value,  # Match the uuid of the dependent (parent_uuid)
                            cl_bundle_dependency.c.child_path == name,  # Match the 'type' of dependent (child_path)
                        )
                    elif key == 'offset':
                        offset = int(value)
                    elif key == 'limit':
                        max_results = int(value)
                    else:
                        clause = and_(
                            cl_bundle_metadata.c.metadata_key == key,
                            cl_bundle_metadata.c.metadata_value == value
                        )
                else:
                    if keyword == 'orphan':
                        # Get bundles with homes (those in worksheets), and then take the complement.
                        with_homes = select([cl_bundle.c.uuid]).where(cl_bundle.c.uuid == cl_worksheet_item.c.bundle_uuid)
                        clause = not_(cl_bundle.c.uuid.in_(with_homes))
                    else:
                        clause = []
                        clause.append(cl_bundle.c.uuid.like('%' + keyword + '%'))
                        clause.append(cl_bundle.c.command.like('%' + keyword + '%'))
                        clause.append(cl_bundle_metadata.c.metadata_value.like('%' + keyword + '%'))
                        clause = or_(*clause)
                clauses.append(clause)
            clause = and_(*clauses)
            clause = and_(clause, cl_bundle.c.uuid == cl_bundle_metadata.c.bundle_uuid)  # Join
            query = select([cl_bundle.c.uuid]).distinct().where(clause).offset(offset).limit(max_results)

        #print 'QUERY', query, query.compile().params
        with self.engine.begin() as connection:
            rows = connection.execute(query).fetchall()
        #for row in rows: print row
        return [row[0] for row in rows]

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

    def add_bundle_action(self, uuid, action):
        with self.engine.begin() as connection:
            connection.execute(cl_bundle_action.insert().values({"bundle_uuid": uuid, "action": action}))

    def add_bundle_actions(self, bundle_actions):
        with self.engine.begin() as connection:
            self.do_multirow_insert(connection, cl_bundle_action, bundle_actions)

    def pop_bundle_actions(self):
        with self.engine.begin() as connection:
            results = connection.execute(cl_bundle_action.select()).fetchall()  # Get the actions
            connection.execute(cl_bundle_action.delete())  # Delete all actions
            return [x for x in results]

    def save_bundle(self, bundle):
        '''
        Save a bundle. On success, sets the Bundle object's id from the result.
        '''
        bundle.validate()
        bundle_value = bundle.to_dict()
        dependency_values = bundle_value.pop('dependencies')
        metadata_values = bundle_value.pop('metadata')

        # Check to see if bundle is already present, as in a local 'cl cp'
        if not self.batch_get_bundles(uuid=bundle.uuid):
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

    def delete_bundles(self, uuids):
        '''
        Delete bundles with the given uuids.
        '''
        with self.engine.begin() as connection:
            # We must delete bundles rows in the opposite order that we create them
            # to avoid foreign-key constraint failures.
            connection.execute(cl_worksheet_item.delete().where(
                cl_worksheet_item.c.bundle_uuid.in_(uuids)
            ))
            connection.execute(cl_bundle_metadata.delete().where(
                cl_bundle_metadata.c.bundle_uuid.in_(uuids)
            ))
            connection.execute(cl_bundle_dependency.delete().where(
                cl_bundle_dependency.c.child_uuid.in_(uuids)
            ))
            connection.execute(cl_bundle.delete().where(
                cl_bundle.c.uuid.in_(uuids)
            ))

    #############################################################################
    # Worksheet-related model methods follow!
    #############################################################################

    def get_worksheet(self, uuid, fetch_items):
        '''
        Get a worksheet given its uuid.
        '''
        worksheets = self.batch_get_worksheets(fetch_items=fetch_items, uuid=uuid)
        if not worksheets:
            raise UsageError('Could not find worksheet with uuid %s' % (uuid,))
        elif len(worksheets) > 1:
            raise IntegrityError('Found multiple workseets with uuid %s' % (uuid,))
        return worksheets[0]

    def batch_get_worksheets(self, fetch_items, **kwargs):
        '''
        Get a list of worksheets, all of which satisfy the clause given by kwargs.
        '''
        base_worksheet_uuid = kwargs.pop('base_worksheet_uuid', None)
        clause = self.make_kwargs_clause(cl_worksheet, kwargs)
        # Handle base_worksheet_uuid specially
        if base_worksheet_uuid:
            clause = and_(clause,
                cl_worksheet_item.c.subworksheet_uuid == cl_worksheet.c.uuid,
                cl_worksheet_item.c.worksheet_uuid == base_worksheet_uuid)

        with self.engine.begin() as connection:
            worksheet_rows = connection.execute(
              cl_worksheet.select().distinct().where(clause)
            ).fetchall()
            if not worksheet_rows:
                if base_worksheet_uuid != None:
                    # We didn't find any results restricting to base_worksheet_uuid,
                    # so do a global search
                    return self.batch_get_worksheets(fetch_items, **kwargs)
                return []
            # Fetch the items of all the worksheets
            if fetch_items:
                uuids = set(row.uuid for row in worksheet_rows)
                item_rows = connection.execute(cl_worksheet_item.select().where(
                  cl_worksheet_item.c.worksheet_uuid.in_(uuids)
                )).fetchall()
        # Make a dictionary for each worksheet with both its main row and its items.
        worksheet_values = {row.uuid: dict(row) for row in worksheet_rows}
        if fetch_items:
            for value in worksheet_values.itervalues():
                value['items'] = []
            for item_row in sorted(item_rows, key=item_sort_key):
                if item_row.worksheet_uuid not in worksheet_values:
                    raise IntegrityError('Got item %s without worksheet' % (item_row,))
                worksheet_values[item_row.worksheet_uuid]['items'].append(item_row)
        return [Worksheet(value) for value in worksheet_values.itervalues()]

    def list_worksheets(self, user_id=None):
        '''
        Return a list of row dicts, one per worksheet. These dicts do NOT contain
        ALL worksheet items; this method is meant to make it easy for a user to see
        their existing worksheets.
        '''
        cols_to_select = [cl_worksheet.c.id,
                          cl_worksheet.c.uuid,
                          cl_worksheet.c.name,
                          cl_worksheet.c.owner_id,
                          cl_group_object_permission.c.permission]
        cols1 = cols_to_select[:4]
        cols1.extend([literal(GROUP_OBJECT_PERMISSION_ALL).label('permission')])
        if user_id == self.root_user_id:
            # query all worksheets
            stmt = select(cols1)
        elif user_id is None:
            # query for public worksheets (only used by the webserver when user is not logged in)
            stmt = select(cols_to_select).\
                where(cl_worksheet.c.uuid == cl_group_object_permission.c.object_uuid).\
                where(cl_group_object_permission.c.group_uuid == self.public_group_uuid)
        else:
            # 1) Worksheets owned by owner_id
            stmt1 = select(cols1).where(cl_worksheet.c.owner_id == user_id)

            # 2) Worksheets visible to owner_id or co-owned by owner_id
            stmt2_groups = select([cl_user_group.c.group_uuid]).\
                where(cl_user_group.c.user_id == user_id)
            # List worksheets where one of our groups has permission.
            stmt2 = select(cols_to_select).\
                where(cl_worksheet.c.uuid == cl_group_object_permission.c.object_uuid).\
                where(or_(
                    cl_group_object_permission.c.group_uuid.in_(stmt2_groups),
                    cl_group_object_permission.c.group_uuid == self.public_group_uuid)).\
                where(cl_worksheet.c.owner_id != user_id)  # Avoid duplicates

            stmt = union(stmt1, stmt2)

        with self.engine.begin() as connection:
            rows = connection.execute(stmt).fetchall()
            if not rows:
                return []

        # Get permissions of the worksheets
        worksheet_uuids = [row.uuid for row in rows]
        uuid_group_permissions = dict(zip(worksheet_uuids, self.batch_get_group_permissions(worksheet_uuids)))

        # Put the permissions into the worksheets
        row_dicts = []
        for row in sorted(rows, key=lambda item: item['id']):
            row = dict(row)
            row['group_permissions'] = uuid_group_permissions[row['uuid']]
            row_dicts.append(row)

        return row_dicts

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
        (bundle_uuid, subworksheet_uuid, value, type) = item
        if value == None: value = ''  # TODO: change tables.py to allow nulls
        item_value = {
          'worksheet_uuid': worksheet_uuid,
          'bundle_uuid': bundle_uuid,
          'subworksheet_uuid': subworksheet_uuid,
          'value': value,
          'type': type,
          'sort_key': None,
        }
        with self.engine.begin() as connection:
            connection.execute(cl_worksheet_item.insert().values(item_value))

    def add_shadow_worksheet_items(self, old_bundle_uuid, new_bundle_uuid):
        '''
        For each occurrence of old_bundle_uuid in any worksheet, add
        new_bundle_uuid right after it (a shadow).
        '''
        with self.engine.begin() as connection:
            # Find all the worksheet_items that old_bundle_uuid appears in
            query = select([cl_worksheet_item.c.worksheet_uuid, cl_worksheet_item.c.sort_key]).where(cl_worksheet_item.c.bundle_uuid == old_bundle_uuid)
            old_items = connection.execute(query)
            #print 'add_shadow_worksheet_items', old_items

            # Go through and insert a worksheet item with new_bundle_uuid after
            # each of the old items.
            new_items = []
            for old_item in old_items:
                new_item = {
                  'worksheet_uuid': old_item.worksheet_uuid,
                  'bundle_uuid': new_bundle_uuid,
                  'type': worksheet_util.TYPE_BUNDLE,
                  'value': '',  # TODO: replace with None once we change tables.py
                  'sort_key': old_item.sort_key,  # Can't really do after, so use the same value.
                }
                new_items.append(new_item)
                connection.execute(cl_worksheet_item.insert().values(new_item))
            # sqlite doesn't support batch insertion
            #connection.execute(cl_worksheet_item.insert().values(new_items))

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
          'subworksheet_uuid': subworksheet_uuid,
          'value': value,
          'type': type,
          'sort_key': (last_item_id + i - len(new_items)),
        } for (i, (bundle_uuid, subworksheet_uuid, value, type)) in enumerate(new_items)]
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

    def chown_worksheet(self, worksheet, owner_id):
        '''
        Update the given worksheet's owner_id.
        '''
        worksheet.owner_id = owner_id
        worksheet.validate()
        with self.engine.begin() as connection:
            connection.execute(cl_worksheet.update().where(
              cl_worksheet.c.uuid == worksheet.uuid
            ).values({'owner_id': owner_id}))

    def delete_worksheet(self, worksheet_uuid):
        '''
        Delete the worksheet with the given uuid.
        '''
        with self.engine.begin() as connection:
            connection.execute(cl_group_object_permission.delete().\
                where(cl_group_object_permission.c.object_uuid == worksheet_uuid)
            )
            connection.execute(cl_worksheet_item.delete().where(
              cl_worksheet_item.c.worksheet_uuid == worksheet_uuid
            ))
            connection.execute(cl_worksheet_item.delete().where(
              cl_worksheet_item.c.subworksheet_uuid == worksheet_uuid
            ))
            connection.execute(cl_worksheet.delete().where(
              cl_worksheet.c.uuid == worksheet_uuid
            ))

    #############################################################################
    # Commands related to groups and permissions follow!
    #############################################################################

    def _create_default_groups(self):
        '''
        Create system-defined groups. This is called by create_tables.
        '''
        groups = self.batch_get_groups(name='public', user_defined=False)
        if len(groups) == 0:
            group_dict = self.create_group({'uuid': spec_util.generate_uuid(),
                                            'name': 'public',
                                            'owner_id': None,
                                            'user_defined': False})
        else:
            group_dict = groups[0]
        self.public_group_uuid = group_dict['uuid']

        # TODO: find a more systematic way of doing this.
        self.root_user_id = '0'

    def list_groups(self, owner_id):
        '''
        Return a list of row dicts --one per group-- for the given owner.
        '''
        with self.engine.begin() as connection:
            rows = connection.execute(cl_group.select().where(
                cl_group.c.owner_id == owner_id
            )).fetchall()
        return [dict(row) for row in sorted(rows, key=lambda row: row.id)]

    def create_group(self, group_dict):
        '''
        Create the group specified by the given row dict.
        '''
        with self.engine.begin() as connection:
            result = connection.execute(cl_group.insert().values(group_dict))
            group_dict['id'] = result.lastrowid
        return group_dict

    def batch_get_groups(self, **kwargs):
        '''
        Get a list of groups, all of which satisfy the clause given by kwargs.
        '''
        clause = self.make_kwargs_clause(cl_group, kwargs)
        with self.engine.begin() as connection:
            rows = connection.execute(
              cl_group.select().where(clause)
            ).fetchall()
            if not rows:
                return []
        values = {row.uuid: dict(row) for row in rows}
        return [value for value in values.itervalues()]

    def batch_get_all_groups(self, spec_filters, group_filters, user_group_filters):
        '''
        Get a list of groups by querying the group table and/or the user_group table.
        Take the union of the two results.  This method performs the general query:

        q1 = select([...]).\
                where(clause_from_spec_filters).\
                where(clause_from_group_filters)
        q2 = select([...]).\
                where(clause_from_spec_filters).\
                where(group.c.uuid == user_group.c.group_uuid).\
                where(clause_from_user_group_filters)
        q = union(s1, s2)
        '''
        fetch_cols1 = [cl_group.c.uuid, cl_group.c.name, cl_group.c.owner_id, cl_group.c.owner_id.label('user_id'), literal(True).label('is_admin')]
        fetch_cols2 = list(fetch_cols1)[:3]
        fetch_cols2.extend([cl_user_group.c.user_id, cl_user_group.c.is_admin])
        q1 = None
        q2 = None
        if spec_filters:
            spec_clause = self.make_kwargs_clause(cl_group, spec_filters)
            q1 = select(fetch_cols1).where(spec_clause)
            q2 = select(fetch_cols2).where(spec_clause).where(cl_group.c.uuid == cl_user_group.c.group_uuid)
        if group_filters:
            group_clause = self.make_kwargs_clause(cl_group, group_filters)
            if q1 is None:
                q1 = select(fetch_cols1)
            q1 = q1.where(group_clause)
        if user_group_filters:
            user_group_clause = self.make_kwargs_clause(cl_user_group, user_group_filters)
            if q2 is None:
                q2 = select(fetch_cols2).where(cl_group.c.uuid == cl_user_group.c.group_uuid)
            q2 = q2.where(user_group_clause)
        # Figure out which query to run: q1, q2, union(q1,q2) or none. Query to execute will be in q1.
        if q1 is None:
            if q2 is None:
                return []
            q1 = q2
        else:
            if q2 is not None:
                q1 = union(q1, q2)
        with self.engine.begin() as connection:
            rows = connection.execute(q1).fetchall()
            if not rows:
                return []
            for i, row in enumerate(rows):
                row = dict(row)
                # TODO: remove these conversions once database schema is changed from int to str
                row['user_id'] = str(row['user_id'])
                row['owner_id'] = str(row['owner_id'])
                rows[i] = row
            values = {row['uuid']: dict(row) for row in rows}
            return [value for value in values.itervalues()]

    def delete_group(self, uuid):
        '''
        Delete the group with the given uuid.
        '''
        with self.engine.begin() as connection:
            connection.execute(cl_group_object_permission.delete().\
                where(cl_group_object_permission.c.group_uuid == uuid)
            )
            connection.execute(cl_user_group.delete().\
                where(cl_user_group.c.group_uuid == uuid)
            )
            connection.execute(cl_group.delete().where(
              cl_group.c.uuid == uuid
            ))

    def add_user_in_group(self, user_id, group_uuid, is_admin):
        '''
        Add user as a member of a group.
        '''
        row = {'group_uuid': group_uuid, 'user_id': user_id, 'is_admin': is_admin}
        with self.engine.begin() as connection:
            result = connection.execute(cl_user_group.insert().values(row))
            row['id'] = result.lastrowid
        return row

    def delete_user_in_group(self, user_id, group_uuid):
        '''
        Add user as a member of a group.
        '''
        with self.engine.begin() as connection:
            connection.execute(cl_user_group.delete().\
                where(cl_user_group.c.user_id == user_id).\
                where(cl_user_group.c.group_uuid == group_uuid)
            )

    def update_user_in_group(self, user_id, group_uuid, is_admin):
        '''
        Add user as a member of a group.
        '''
        with self.engine.begin() as connection:
            connection.execute(cl_user_group.update().\
                where(cl_user_group.c.user_id == user_id).\
                where(cl_user_group.c.group_uuid == group_uuid).\
                values({'is_admin': is_admin}))

    def batch_get_user_in_group(self, **kwargs):
        '''
        Return list of user-group entries matching the specified |kwargs|.
        Can be used to get groups for a user or users in a group.
        Examples: user_id=..., group_uuid=...
        '''
        clause = self.make_kwargs_clause(cl_user_group, kwargs)
        with self.engine.begin() as connection:
            rows = connection.execute(
              cl_user_group.select().where(clause)
            ).fetchall()
            if not rows:
                return []
        return [dict(row) for row in rows]

    def add_permission(self, group_uuid, object_uuid, permission):
        '''
        Add specified permission for the given (group, object) pair.
        '''
        row = {'group_uuid': group_uuid, 'object_uuid': object_uuid, 'permission': permission}
        with self.engine.begin() as connection:
            result = connection.execute(cl_group_object_permission.insert().values(row))
            row['id'] = result.lastrowid
        return row

    def delete_permission(self, group_uuid, object_uuid):
        '''
        Delete permissions for the given (group, object) pair.
        '''
        with self.engine.begin() as connection:
            connection.execute(cl_group_object_permission.delete().\
                where(cl_group_object_permission.c.group_uuid == group_uuid).\
                where(cl_group_object_permission.c.object_uuid == object_uuid)
            )

    def update_permission(self, group_uuid, object_uuid, permission):
        '''
        Update permission for the given (group, object) pair.
        There should be one.
        '''
        with self.engine.begin() as connection:
            connection.execute(cl_group_object_permission.update().\
                where(cl_group_object_permission.c.group_uuid == group_uuid).\
                where(cl_group_object_permission.c.object_uuid == object_uuid).\
                values({'permission': permission}))

    def batch_get_group_permissions(self, object_uuids):
        '''
        Return list of sublists (one for each object_uuid), where each sublist
        is a list of {group_uuid: ..., group_name: ..., permission: ...}
        entries for the given objects.  Objects are worksheets.
        '''
        with self.engine.begin() as connection:
            rows = connection.execute(select([cl_group_object_permission, cl_group.c.name])
                .where(cl_group_object_permission.c.group_uuid == cl_group.c.uuid)
                .where(cl_group_object_permission.c.object_uuid.in_(object_uuids))
            ).fetchall()
            result = collections.defaultdict(list)  # object_uuid => list of rows
            for row in rows:
                result[row.object_uuid].append({'group_uuid': row.group_uuid, 'group_name': row.name, 'permission': row.permission})
            return [result[object_uuid] for object_uuid in object_uuids]

    def get_group_permissions(self, object_uuid):
        '''
        Return list of {group_uuid: ..., group_name: ..., permission: ...} entries for the given object.
        Objects are worksheets.
        '''
        return self.batch_get_group_permissions([object_uuid])[0]

    def get_group_permission(self, group_uuid, object_uuid):
        '''
        Get permission for the given (group, object) pair.
        Objects are worksheets.
        '''
        for row in self.get_group_permissions(object_uuid):
            if row['group_uuid'] == group_uuid:
                return row['permission']
        return GROUP_OBJECT_PERMISSION_NONE

    def get_user_permission(self, user_id, object_uuid, object_owner_id):
        '''
        Gets the set of permissions granted to the given user on the given object.
        Use user_id = None to check the set of permissions of an anonymous user.
        Objects are worksheets.
        '''
        # Owner always has all permissions.
        if user_id == object_owner_id:
            return GROUP_OBJECT_PERMISSION_ALL

        # Root always has all permissions.
        if user_id == self.root_user_id:
            return GROUP_OBJECT_PERMISSION_ALL

        # Figure out which groups the user is in (not that many).
        groups = self._get_user_groups(user_id)

        # See if any of these groups have the desired permission.
        group_permissions = self.get_group_permissions(object_uuid)
        permissions = [row['permission'] for row in group_permissions if row['group_uuid'] in groups]
        return max(permissions) if len(permissions) > 0 else GROUP_OBJECT_PERMISSION_NONE
    
    def get_user_permission_on_bundles(self, user_id, bundle_uuids):
        '''
        Return list of permissions for bundle_uuids.
        '''
        # Root always has all permissions.
        if user_id == self.root_user_id:
            return [GROUP_OBJECT_PERMISSION_ALL] * len(bundle_uuids)

        # Start out with no permissions
        permissions = {}
        for uuid in bundle_uuids:
            permissions[uuid] = GROUP_OBJECT_PERMISSION_NONE

        # Read: if there exists a group and worksheet that connects the user and the bundle.
        # Note: might not need this since get_bundle_uuids is already covered by this.
        groups = self._get_user_groups(user_id)
        with self.engine.begin() as connection:
            rows = connection.execute(select([
                cl_worksheet_item.c.bundle_uuid,
                cl_group_object_permission.c.permission
            ]).where(and_(
                cl_group_object_permission.c.group_uuid.in_(groups),
                cl_group_object_permission.c.object_uuid == cl_worksheet_item.c.worksheet_uuid,  # group <=> worksheet
                cl_worksheet_item.c.bundle_uuid.in_(bundle_uuids),  # worksheet <=> bundle
            ))).fetchall()
            for r in rows:
                permissions[r.bundle_uuid] = max(permissions[r.bundle_uuid], r.permission)

        # All: if the user is the owner of the bundle (a bit restrictive for now).
        with self.engine.begin() as connection:
            rows = connection.execute(select([
              cl_bundle.c.uuid,
              cl_bundle.c.owner_id
            ]).where(cl_bundle.c.uuid.in_(bundle_uuids))).fetchall()
            for r in rows:
                if r.owner_id == user_id:
                    permissions[i] = max(permissions[i], GROUP_OBJECT_PERMISSION_ALL)

        return [permissions[uuid] for uuid in bundle_uuids]

    # Helper function: return list of group uuids that |user_id| is in.
    def _get_user_groups(self, user_id):
        groups = [self.public_group_uuid]  # Everyone is in the public group implicitly.
        if user_id != None:
            groups += [row['group_uuid'] for row in self.batch_get_user_in_group(user_id=user_id)]
        return groups 

"""
BundleModel is a wrapper around database calls to save and load bundle metadata.
"""

import collections
import datetime
import os
import re
import time
import logging
import json

from dateutil import parser
from uuid import uuid4

from sqlalchemy import and_, or_, not_, select, union, desc, func
from sqlalchemy.sql.expression import literal, true

from codalab.bundles import get_bundle_subclass
from codalab.bundles.run_bundle import RunBundle
from codalab.common import IntegrityError, NotFoundError, precondition, UsageError
from codalab.lib import crypt_util, spec_util, worksheet_util, path_util
from codalab.model.util import LikeQuery
from codalab.model.tables import (
    bundle as cl_bundle,
    bundle_dependency as cl_bundle_dependency,
    bundle_metadata as cl_bundle_metadata,
    group as cl_group,
    group_bundle_permission as cl_group_bundle_permission,
    group_object_permission as cl_group_worksheet_permission,
    NOTIFICATIONS_GENERAL,
    GROUP_OBJECT_PERMISSION_ALL,
    GROUP_OBJECT_PERMISSION_READ,
    GROUP_OBJECT_PERMISSION_NONE,
    user_group as cl_user_group,
    worksheet as cl_worksheet,
    worksheet_tag as cl_worksheet_tag,
    worksheet_item as cl_worksheet_item,
    user as cl_user,
    chat as cl_chat,
    user_verification as cl_user_verification,
    user_reset_code as cl_user_reset_code,
    oauth2_client,
    oauth2_token,
    oauth2_auth_code,
    worker as cl_worker,
    worker_run as cl_worker_run,
    db_metadata,
)
from codalab.objects.worksheet import item_sort_key, Worksheet
from codalab.objects.oauth2 import OAuth2AuthCode, OAuth2Client, OAuth2Token
from codalab.objects.user import User
from codalab.objects.dependency import Dependency
from codalab.rest.util import get_group_info
from codalab.worker.bundle_state import State

logger = logging.getLogger(__name__)

SEARCH_KEYWORD_REGEX = re.compile('^([\.\w/]*)=(.*)$')
SEARCH_RESULTS_LIMIT = 10


def str_key_dict(row):
    """
    row comes out of an element of a database query.
    For some versions of SqlAlchemy, the keys are of type sqlalchemy.sql.elements.quoted_name,
    which cannot be serialized to JSON.
    This function converts the keys to strings.
    """
    return dict((str(k), v) for k, v in row.items())


class BundleModel(object):
    def __init__(self, engine, default_user_info, root_user_id, system_user_id):
        """
        Initialize a BundleModel with the given SQLAlchemy engine.
        """
        self.engine = engine
        self.default_user_info = default_user_info
        self.root_user_id = root_user_id
        self.system_user_id = system_user_id
        self.public_group_uuid = ''
        self.create_tables()

    # ==========================================================================
    # Database helper methods
    # ==========================================================================

    # TODO: Remove these methods below when all appropriate table columns have
    # been converted to the appropriate types that perform automatic encoding.
    # (See tables.py for more details.)

    def encode_str(self, value):
        raise NotImplementedError

    def decode_str(self, value):
        raise NotImplementedError

    def _reset(self):
        """
        Do a drop / create table to clear and reset the schema of all tables.
        """
        # Do not run this function in production!
        db_metadata.drop_all(self.engine)
        self.create_tables()

    def create_tables(self):
        """
        Create all CodaLab bundle tables if they do not already exist.
        """
        db_metadata.create_all(self.engine)
        self._create_default_groups()
        self._create_default_clients()

    @staticmethod
    def do_multirow_insert(connection, table, values):
        """
        Insert multiple rows into the given table.
        This method may be overridden by models that use more powerful SQL dialects.
        """
        # This is a lowest-common-denominator implementation of a multi-row insert.
        # It deals with a couple of SQL dialect issues:
        #   - Some dialects do not support empty inserts, so we test 'if values'.
        #   - Some dialects do not support multiple inserts in a single statement,
        #     which we deal with by using the DBAPI execute_many pattern.
        if values:
            with connection.begin():
                connection.execute(table.insert(), values)

    @staticmethod
    def make_clause(key, value):
        if isinstance(value, (list, set, tuple)):
            if not value:
                return False
            return key.in_(value)
        if isinstance(value, LikeQuery):
            return key.like(value)
        return key == value

    def make_kwargs_clause(self, table, kwargs):
        """
        Return a list of bundles given a dict mapping table columns to values.
        If a value is a list, set, or tuple, produce an IN clause on that column.
        If a value is a LikeQuery, produce a LIKE clause on that column.
        """
        clauses = [true()]
        for (key, value) in kwargs.items():
            clauses.append(self.make_clause(getattr(table.c, key), value))
        return and_(*clauses)

    @staticmethod
    def _render_query(query):
        """
        Return string representing SQL query.
        """
        query = query.compile()
        s = str(query)
        for k, v in query.params.items():
            s = s.replace(':' + k, str(v))
        return s

    def _execute_query(self, query):
        """
        Execute the given query and return the first matching row
        """
        with self.engine.begin() as connection:
            rows = connection.execute(query).fetchall()
        return [row[0] for row in rows]

    # ==========================================================================
    # Bundle info accessor methods
    # ==========================================================================

    def get_bundle(self, uuid):
        """
        Retrieve a bundle from the database given its uuid.
        Assume it's unique.
        """
        bundles = self.batch_get_bundles(uuid=uuid)
        if not bundles:
            raise NotFoundError('Could not find bundle with uuid %s' % (uuid,))
        if len(bundles) > 1:
            raise IntegrityError('Found multiple bundles with uuid %s' % (uuid,))
        return bundles[0]

    def get_bundle_names(self, uuids):
        """
        Fetch the bundle names of the given uuids.
        Return {uuid: name}
        """
        return self.get_bundle_metadata(uuids, "name")

    def get_bundle_metadata(self, uuids, metadata_key):
        """
        Fetch a single metadata value from the bundles referenced
        by the given uuids.
        Return {uuid: metadata_value}
        """
        if len(uuids) == 0:
            return []
        with self.engine.begin() as connection:
            rows = connection.execute(
                select(
                    [cl_bundle_metadata.c.bundle_uuid, cl_bundle_metadata.c.metadata_value]
                ).where(
                    and_(
                        cl_bundle_metadata.c.metadata_key == metadata_key,
                        cl_bundle_metadata.c.bundle_uuid.in_(uuids),
                    )
                )
            ).fetchall()
            return dict((row.bundle_uuid, row.metadata_value) for row in rows)

    def get_owner_ids(self, table, uuids):
        """
        Fetch the owners of the given uuids (for either bundles or worksheets).
        Return {uuid: ..., owner_id: ...}
        """
        if len(uuids) == 0:
            return []
        with self.engine.begin() as connection:
            rows = connection.execute(
                select([table.c.uuid, table.c.owner_id]).where(table.c.uuid.in_(uuids))
            ).fetchall()
            return dict((row.uuid, row.owner_id) for row in rows)

    def get_bundle_owner_ids(self, uuids):
        return self.get_owner_ids(cl_bundle, uuids)

    def get_worksheet_owner_ids(self, uuids):
        return self.get_owner_ids(cl_worksheet, uuids)

    def get_bundle_worker(self, uuid):
        """
        Returns information about the worker that the given bundle is running
        on. This method should be called only for bundles that are running.
        """
        with self.engine.begin() as conn:
            row = conn.execute(
                cl_worker_run.select().where(cl_worker_run.c.run_uuid == uuid)
            ).fetchone()

            if not row:
                logger.info('Trying to find worker for bundle {} that is not running.'.format(uuid))
                return None

            worker_row = conn.execute(
                cl_worker.select().where(
                    and_(cl_worker.c.user_id == row.user_id, cl_worker.c.worker_id == row.worker_id)
                )
            ).fetchone()
            return {
                'user_id': worker_row.user_id,
                'worker_id': worker_row.worker_id,
                'shared_file_system': worker_row.shared_file_system,
                'socket_id': worker_row.socket_id,
            }

    def get_children_uuids(self, uuids):
        """
        Get all bundles that depend on the bundle with the given uuids.
        Return {parent_uuid: [child_uuid, ...], ...}
        """
        with self.engine.begin() as connection:
            rows = connection.execute(
                select(
                    [cl_bundle_dependency.c.parent_uuid, cl_bundle_dependency.c.child_uuid]
                ).where(cl_bundle_dependency.c.parent_uuid.in_(uuids))
            ).fetchall()
        result = dict((uuid, []) for uuid in uuids)
        for row in rows:
            result[row.parent_uuid].append(row.child_uuid)
        return result

    def get_host_worksheet_uuids(self, bundle_uuids, max_worksheets):
        """
        Get up to n host_worksheet uuids per bundle uuid. n of 0 will return an empty dictionary.
        bundle_uuids: list of bundle uuid's (e.g. ['0x12345', '0x23456'])
        max_worksheets: max limit of host_worksheet uuid's to fetch per bundle
        Return dict of bundle uuid's to a list of host worksheet uuid's {'0x12345': [host_worksheet_uuid, ...], ...}
        """
        if max_worksheets < 0:
            raise ValueError('Invalid n: {}. n has to be 0 or greater.'.format(max_worksheets))
        if max_worksheets == 0:
            return dict()

        with self.engine.begin() as connection:
            rows = connection.execute(
                select(
                    [
                        cl_worksheet_item.c.bundle_uuid,
                        func.substring_index(
                            func.group_concat(cl_worksheet_item.c.worksheet_uuid),
                            ',',
                            max_worksheets,
                        ).label('worksheet_uuids'),
                    ]
                )
                .where(cl_worksheet_item.c.bundle_uuid.in_(bundle_uuids))
                .group_by(cl_worksheet_item.c.bundle_uuid)
            ).fetchall()
        return dict((row.bundle_uuid, row.worksheet_uuids.split(',')) for row in rows)

    def get_all_host_worksheet_uuids(self, bundle_uuids):
        """
        Return list of all worksheet uuids that contain the given bundle_uuids.
        bundle_uuids: list of bundle uuid's (e.g. ['0x12345', '0x23456']
        Return dict of bundle uuid's to a list of host worksheet uuid's {'0x12345': [host_worksheet_uuid, ...], ...}
        """
        with self.engine.begin() as connection:
            rows = connection.execute(
                select([cl_worksheet_item.c.worksheet_uuid, cl_worksheet_item.c.bundle_uuid]).where(
                    cl_worksheet_item.c.bundle_uuid.in_(bundle_uuids)
                )
            ).fetchall()
        result = dict((uuid, []) for uuid in bundle_uuids)
        for row in rows:
            result[row.bundle_uuid].append(row.worksheet_uuid)
        # Deduplicate entries
        for uuid in result.keys():
            result[uuid] = list(set(result[uuid]))
        return result

    def get_self_and_descendants(self, uuids, depth):
        """
        Get all bundles that depend on bundles with the given uuids.
        depth = 1 gets only children
        """
        frontier = uuids
        visited = list(frontier)
        while len(frontier) > 0 and depth > 0:
            # Get children of all nodes in frontier
            result = self.get_children_uuids(frontier)
            new_frontier = []
            for v in result.values():
                for uuid in v:
                    if uuid in visited:
                        continue
                    new_frontier.append(uuid)
                    visited.append(uuid)
            frontier = new_frontier
            depth -= 1
        return visited

    def search_bundles(self, user_id, keywords):
        """
        Returns a bundle search result dict where:
            result: list of bundle uuids matching search criteria in order
                          specified for bundle searches
                    single number value for aggregate searches(.count, .sum)
            is_aggregate: True for aggregate searches, False otherwise
        Each keyword is either:
        - <key>=<value>
        - .floating: return bundles not in any worksheet
        - .offset=<int>: return bundles starting at this offset
        - .limit=<int>: maximum number of bundles to return
        - .count: just return the number of bundles
        - .shared: shared with me through a group
        - .mine: sugar for owner_id=user_id
        - .last: sugar for id=.sort-
        Keys are one of the following:
        - Bundle fields (e.g., uuid)
        - Metadata fields (e.g., time)
        - Special fields (e.g., dependencies)
        Values can be one of the following:
        - .sort: sort in increasing order
        - .sort-: sort by decreasing order
        - .sum: add up the numbers
        Bare keywords: sugar for uuid_name=.*<word>.*
        Search only bundles which are readable by user_id.
        """
        clauses = []
        offset = 0
        limit = SEARCH_RESULTS_LIMIT
        format_func = None
        count = False
        sort_key = [None]
        sum_key = [None]
        aux_fields = []  # Fields (e.g., sorting) that we need to include in the query

        # Number nested subqueries
        subquery_index = [0]

        def alias(clause):
            subquery_index[0] += 1
            return clause.alias('q' + str(subquery_index[0]))

        def is_numeric(key):
            return key != 'name'

        def make_condition(key, field, value):
            # Special
            if value == '.sort':
                aux_fields.append(field)
                if is_numeric(key):
                    field = field * 1
                sort_key[0] = field
            elif value == '.sort-':
                aux_fields.append(field)
                if is_numeric(key):
                    field = field * 1
                sort_key[0] = desc(field)
            elif value == '.sum':
                sum_key[0] = field * 1
            else:
                # Ordinary value
                if isinstance(value, list):
                    return field.in_(value)
                if '%' in value:
                    return field.like(value)
                return field == value
            return None

        shortcuts = {'type': 'bundle_type', 'size': 'data_size', 'worksheet': 'host_worksheet'}

        for keyword in keywords:
            keyword = keyword.replace('.*', '%')
            # Sugar
            if keyword == '.mine':
                keyword = 'owner_id=' + (user_id or '')
            elif keyword == '.shared':
                keyword = '.shared=True'
            elif keyword == '.last':
                keyword = 'id=.sort-'
            elif keyword == '.count':
                count = True
                limit = None
                continue
            elif keyword == '.floating':
                # Get bundles that have host worksheets, and then take the complement.
                with_hosts = alias(
                    select([cl_bundle.c.uuid]).where(
                        cl_bundle.c.uuid == cl_worksheet_item.c.bundle_uuid
                    )
                )
                clause = not_(cl_bundle.c.uuid.in_(with_hosts))
                clauses.append(clause)
                continue

            m = SEARCH_KEYWORD_REGEX.match(keyword)  # key=value
            if m:
                key, value = m.group(1), m.group(2)
                key = shortcuts.get(key, key)
                if ',' in value:  # value is value1,value2
                    value = value.split(',')
            else:
                key, value = 'uuid_name', keyword

            clause = None
            # Special functions
            if key == '.offset':
                offset = int(value)
            elif key == '.limit':
                limit = int(value)
            elif key == '.format':
                format_func = value
            # Bundle fields
            elif key in ('bundle_type', 'id', 'uuid', 'data_hash', 'state', 'command', 'owner_id'):
                clause = make_condition(key, getattr(cl_bundle.c, key), value)
            elif key == '.shared':  # shared with any group I am in with read permission
                clause = cl_bundle.c.uuid.in_(
                    select([cl_group_bundle_permission.c.object_uuid]).where(
                        and_(
                            cl_group_bundle_permission.c.group_uuid.in_(
                                alias(
                                    select([cl_user_group.c.group_uuid]).where(
                                        cl_user_group.c.user_id == user_id
                                    )
                                )
                            ),
                            cl_group_bundle_permission.c.permission >= GROUP_OBJECT_PERMISSION_READ,
                        )
                    )
                )
            elif key == 'group':  # shared with group with read permission
                group_uuid = get_group_info(value, False)['uuid']
                clause = cl_bundle.c.uuid.in_(
                    select([cl_group_bundle_permission.c.object_uuid]).where(
                        and_(
                            cl_group_bundle_permission.c.group_uuid == group_uuid,
                            cl_group_bundle_permission.c.permission >= GROUP_OBJECT_PERMISSION_READ,
                        )
                    )
                )
            # Special fields
            elif key == 'dependency':
                # Match uuid of dependency
                condition = make_condition(key, cl_bundle_dependency.c.parent_uuid, value)
                if condition is None:  # top-level
                    clause = cl_bundle_dependency.c.child_uuid == cl_bundle.c.uuid
                else:  # embedded
                    clause = cl_bundle.c.uuid.in_(
                        alias(select([cl_bundle_dependency.c.child_uuid]).where(condition))
                    )
            elif key.startswith('dependency/'):
                _, name = key.split('/', 1)
                condition = make_condition(key, cl_bundle_dependency.c.parent_uuid, value)
                if condition is None:  # top-level
                    clause = and_(
                        cl_bundle_dependency.c.child_uuid == cl_bundle.c.uuid,  # Join constraint
                        cl_bundle_dependency.c.child_path
                        == name,  # Match the 'type' of dependent (child_path)
                    )
                else:  # embedded
                    clause = cl_bundle.c.uuid.in_(
                        alias(
                            select([cl_bundle_dependency.c.child_uuid]).where(
                                and_(
                                    cl_bundle_dependency.c.child_path
                                    == name,  # Match the 'type' of dependent (child_path)
                                    condition,
                                )
                            )
                        )
                    )
            elif key == 'host_worksheet':
                condition = make_condition(key, cl_worksheet_item.c.worksheet_uuid, value)
                if condition is None:  # top-level
                    clause = cl_worksheet_item.c.bundle_uuid == cl_bundle.c.uuid  # Join constraint
                else:
                    clause = cl_bundle.c.uuid.in_(
                        alias(select([cl_worksheet_item.c.bundle_uuid]).where(condition))
                    )
            elif key in ('.before', '.after'):
                target_datetime = parser.isoparse(value)

                subclause = None
                if key == '.before':
                    subclause = cl_bundle_metadata.c.metadata_value <= int(
                        target_datetime.timestamp()
                    )
                if key == '.after':
                    subclause = cl_bundle_metadata.c.metadata_value >= int(
                        target_datetime.timestamp()
                    )

                clause = cl_bundle.c.uuid.in_(
                    alias(
                        select([cl_bundle_metadata.c.bundle_uuid]).where(
                            and_(cl_bundle_metadata.c.metadata_key == 'created', subclause)
                        )
                    )
                )
            elif key == 'uuid_name':  # Search uuid and name by default
                clause = []
                clause.append(cl_bundle.c.uuid.like('%' + value + '%'))
                clause.append(
                    cl_bundle.c.uuid.in_(
                        alias(
                            select([cl_bundle_metadata.c.bundle_uuid]).where(
                                and_(
                                    cl_bundle_metadata.c.metadata_key == 'name',
                                    cl_bundle_metadata.c.metadata_value.like('%' + value + '%'),
                                )
                            )
                        )
                    )
                )
                clause = or_(*clause)
            elif key == '':  # Match any field
                clause = []
                clause.append(cl_bundle.c.uuid.like('%' + value + '%'))
                clause.append(cl_bundle.c.command.like('%' + value + '%'))
                clause.append(
                    cl_bundle.c.uuid.in_(
                        alias(
                            select([cl_bundle_metadata.c.bundle_uuid]).where(
                                cl_bundle_metadata.c.metadata_value.like('%' + value + '%')
                            )
                        )
                    )
                )
                clause = or_(*clause)
            # Otherwise, assume metadata.
            else:
                condition = make_condition(key, cl_bundle_metadata.c.metadata_value, value)
                if condition is None:  # top-level
                    clause = and_(
                        cl_bundle.c.uuid == cl_bundle_metadata.c.bundle_uuid,
                        cl_bundle_metadata.c.metadata_key == key,
                    )
                else:  # embedded
                    clause = cl_bundle.c.uuid.in_(
                        select([cl_bundle_metadata.c.bundle_uuid]).where(
                            and_(cl_bundle_metadata.c.metadata_key == key, condition)
                        )
                    )

            if clause is not None:
                clauses.append(clause)

        clause = and_(*clauses)

        if user_id != self.root_user_id:
            # Restrict to the bundles that we have access to.
            access_via_owner = cl_bundle.c.owner_id == user_id
            access_via_group = cl_bundle.c.uuid.in_(
                select([cl_group_bundle_permission.c.object_uuid]).where(
                    and_(
                        or_(  # Join constraint (group)
                            cl_group_bundle_permission.c.group_uuid
                            == self.public_group_uuid,  # Public group
                            cl_group_bundle_permission.c.group_uuid.in_(
                                alias(
                                    select([cl_user_group.c.group_uuid]).where(
                                        cl_user_group.c.user_id == user_id
                                    )
                                )
                            ),  # Private group
                        ),
                        cl_group_bundle_permission.c.permission
                        >= GROUP_OBJECT_PERMISSION_READ,  # Match the uuid of the parent
                    )
                )
            )
            clause = and_(clause, or_(access_via_owner, access_via_group))

        # Aggregate (sum)
        if sum_key[0] is not None:
            # Construct a table with only the uuid and the num (and make sure it's distinct!)
            query = alias(
                select([cl_bundle.c.uuid, sum_key[0].label('num')]).distinct().where(clause)
            )
            # Sum the numbers
            query = select([func.sum(query.c.num)])
        else:
            query = (
                select([cl_bundle.c.uuid] + aux_fields)
                .distinct()
                .where(clause)
                .offset(offset)
                .limit(limit)
            )

        # Sort
        if sort_key[0] is not None:
            query = query.order_by(sort_key[0])

        # Count
        if count:
            query = alias(query).count()

        result = self._execute_query(query)
        if count or sum_key[0] is not None:  # Just returning a single number
            result = worksheet_util.apply_func(format_func, result[0])
            return {'result': result, 'is_aggregate': True}
        return {'result': result, 'is_aggregate': False}

    def get_bundle_uuids(self, conditions, max_results):
        """
        Returns a list of bundle_uuids that have match the conditions.
        Possible conditions on bundles: uuid, name, worksheet_uuid
        """
        if 'uuid' in conditions:
            # Match the uuid only
            clause = self.make_clause(cl_bundle.c.uuid, conditions['uuid'])
            query = select([cl_bundle.c.uuid]).where(clause)
        elif 'name' in conditions:
            # Select name
            if conditions['name']:
                clause = and_(
                    cl_bundle_metadata.c.metadata_key == 'name',
                    self.make_clause(cl_bundle_metadata.c.metadata_value, conditions['name']),
                )
            else:
                clause = true()

            if conditions['worksheet_uuid']:
                # Select things on the given worksheet
                # WARNING: Will also include invalid bundle ids that are listed on the worksheet
                clause = and_(
                    clause,
                    self.make_clause(
                        cl_worksheet_item.c.worksheet_uuid, conditions['worksheet_uuid']
                    ),
                )
                clause = and_(clause, cl_worksheet_item.c.bundle_uuid.isnot(None))
                join = cl_worksheet_item.outerjoin(
                    cl_bundle_metadata,
                    cl_worksheet_item.c.bundle_uuid == cl_bundle_metadata.c.bundle_uuid,
                )
                query = (
                    select([cl_worksheet_item.c.bundle_uuid, cl_worksheet_item.c.id])
                    .select_from(join)
                    .distinct()
                    .where(clause)
                )
                query = query.order_by(cl_worksheet_item.c.id.desc()).limit(max_results)
            else:
                if not conditions['name']:
                    raise UsageError('Nothing is specified')
                # Select from all bundles
                clause = and_(clause, cl_bundle.c.uuid == cl_bundle_metadata.c.bundle_uuid)  # Join
                query = select([cl_bundle.c.uuid]).where(clause)
                query = query.order_by(cl_bundle.c.id.desc()).limit(max_results)

        return self._execute_query(query)

    def get_memoized_bundles(self, user_id, command, dependencies):
        """
        Get a list of bundle UUIDs that match with input command and dependencies in the order of they were created.
        :param user_id: a string that specifies the current user id.
        :param command: a string that defines the command that is used to search for memoized bundles in the database.
        :param dependencies: a string in the form of '[{"child_path": key1, "parent_uuid": uuid1},
                                                       {"child_path": key2, "parent_uuid": uuid2}]'
                            to search for matched dependencies in the database.
        :return: a list of matched UUIDs.
        """
        # Decode json formatted dependencies string to a list of key value pairs
        dependencies = json.loads(dependencies)
        # When there is no dependency to be matched, the target memoized bundle
        # should only exist in the bundle table but not in the bundle_dependency table.
        if len(dependencies) == 0:
            query = (
                select([cl_bundle.c.uuid])
                .select_from(cl_bundle)
                .where(
                    and_(
                        cl_bundle.c.command == command,
                        cl_bundle.c.owner_id == user_id,
                        cl_bundle.c.uuid.notin_(
                            select([cl_bundle_dependency.c.child_uuid]).select_from(
                                cl_bundle_dependency
                            )
                        ),
                    )
                )
                .order_by(cl_bundle.c.id)
            )
        else:
            # The following matching logic contains two aggregations. In the first aggregation, we select those records
            # that have the same number of dependencies as specified in input. In the second aggregation, we operate on
            # records that returned from the first aggregation. We first select those records that match with all
            # (child_path, parent_uuid) dependency pairs from the input. Then, we aggregate on child_uuid and match
            # the total the number of unique dependencies per child_uuid with input dependencies.
            clause = []
            for dep in dependencies:
                clause.append(
                    and_(
                        cl_bundle_dependency.c.child_path == dep['child_path'],
                        cl_bundle_dependency.c.parent_uuid == dep['parent_uuid'],
                    )
                )
            # Step 1: filter by input command and the number of dependencies
            command_filter = (
                select([cl_bundle_dependency.c.child_uuid])
                .select_from(
                    cl_bundle.join(
                        cl_bundle_dependency, cl_bundle.c.uuid == cl_bundle_dependency.c.child_uuid
                    )
                )
                .where(and_(cl_bundle.c.command == command, cl_bundle.c.owner_id == user_id))
                # child_path is unique across all dependencies, aggregate on child_uuid
                # and COUNT the total the number of unique dependencies per child_uuid
                .group_by(cl_bundle_dependency.c.child_uuid)
                .having(func.count(cl_bundle_dependency.c.child_path) == len(dependencies))
            )
            uuids = self._execute_query(command_filter)

            # Step 2: filter by each dependency (child_path, parent_uuid) pair in the bundle_dependency table
            query = (
                select([cl_bundle_dependency.c.child_uuid])
                .select_from(cl_bundle_dependency)
                .where(and_(cl_bundle_dependency.c.child_uuid.in_(uuids), or_(*clause)))
                # child_path is unique across all dependencies, aggregate on child_uuid
                # and COUNT the total the number of unique dependencies per child_uuid
                .group_by(cl_bundle_dependency.c.child_uuid)
                .having(func.count(cl_bundle_dependency.c.child_path) == len(dependencies))
                # Ensure the order of the returning bundles will be in the order of they were created.
                .order_by(cl_bundle_dependency.c.id)
            )

        return self._execute_query(query)

    def batch_get_bundles(self, **kwargs):
        """
        Return a list of bundles given a SQLAlchemy clause on the cl_bundle table.
        """
        clause = self.make_kwargs_clause(cl_bundle, kwargs)
        with self.engine.begin() as connection:
            bundle_rows = connection.execute(cl_bundle.select().where(clause)).fetchall()
            if not bundle_rows:
                return []
            uuids = set(bundle_row.uuid for bundle_row in bundle_rows)
            dependency_rows = connection.execute(
                cl_bundle_dependency.select()
                .where(cl_bundle_dependency.c.child_uuid.in_(uuids))
                .order_by(cl_bundle_dependency.c.id)
            ).fetchall()
            metadata_rows = connection.execute(
                cl_bundle_metadata.select().where(cl_bundle_metadata.c.bundle_uuid.in_(uuids))
            ).fetchall()

        # Make a dictionary for each bundle with both data and metadata.
        bundle_values = {row.uuid: str_key_dict(row) for row in bundle_rows}
        for bundle_value in bundle_values.values():
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
        sorted_values = sorted(bundle_values.values(), key=lambda r: r['id'])
        bundles = [
            #
            get_bundle_subclass(bundle_value['bundle_type'])(bundle_value)
            for bundle_value in sorted_values
        ]
        return bundles

    # ==========================================================================
    # Server-side bundle state machine methods
    # ==========================================================================

    def transition_bundle_starting(self, bundle, user_id, worker_id):
        """
        Transitions bundle to STARTING state:
            Updates the last_updated metadata.
            Adds a worker_run row that tracks which worker will run the bundle.
        """
        with self.engine.begin() as connection:
            # Check if the requested bundle still exists.
            row = connection.execute(
                cl_bundle.select().where(cl_bundle.c.id == bundle.id)
            ).fetchone()
            if not row:
                # The user deleted the bundle.
                return False

            # Check if the designated worker is going to be terminated soon
            row = connection.execute(
                cl_worker.select().where(
                    and_(
                        cl_worker.c.worker_id == worker_id,
                        cl_worker.c.is_terminating == False,  # NOQA E712
                    )
                )
            ).fetchone()
            # If the worker is going to be terminated soon, stop starting bundle on this worker
            if not row:
                return False

            bundle_update = {
                'state': State.STARTING,
                'metadata': {'last_updated': int(time.time())},
            }
            self.update_bundle(bundle, bundle_update, connection)

            worker_run_row = {'user_id': user_id, 'worker_id': worker_id, 'run_uuid': bundle.uuid}
            connection.execute(cl_worker_run.insert().values(worker_run_row))

            return True

    def transition_bundle_staged(self, bundle):
        """
        Transitions bundle to STAGED state:
            Returns False if the bundle was not in STARTING state.
            Clears the job_handle metadata and removes the worker_run row.
        """
        with self.engine.begin() as connection:
            # Make sure it's still starting.
            row = connection.execute(
                cl_bundle.select().where(cl_bundle.c.id == bundle.id)
            ).fetchone()
            if not row:
                raise IntegrityError('Missing bundle with UUID %s' % bundle.uuid)

            # Reset all metadata fields that aren't input by user from RunBundle class to be None.
            # Excluding all the fields that can be set by users, which for now is just the "actions" field.
            # Excluding the "created" field to keep track of the original date when the bundle is created
            metadata_update = {
                spec.key: None
                for spec in RunBundle.METADATA_SPECS
                if spec.generated and spec.key not in ['actions', 'created']
            }
            bundle_update = {'state': State.STAGED, 'metadata': metadata_update}
            self.update_bundle(bundle, bundle_update, connection)
            connection.execute(
                cl_worker_run.delete().where(cl_worker_run.c.run_uuid == bundle.uuid)
            )
            return True

    def transition_bundle_preparing(self, bundle, user_id, worker_id, start_time, remote):
        """
        Transitions bundle to PREPARING state:
            Only if the bundle is still scheduled to run on the given worker
            (done by checking the worker_run table).
            Returns True if it is.
        """
        with self.engine.begin() as connection:
            # Check that still assigned to this worker.
            run_row = connection.execute(
                cl_worker_run.select().where(cl_worker_run.c.run_uuid == bundle.uuid)
            ).fetchone()
            if not run_row or run_row.user_id != user_id or run_row.worker_id != worker_id:
                return False

            bundle_update = {
                'state': State.PREPARING,
                'metadata': {'started': start_time, 'last_updated': start_time, 'remote': remote},
            }
            self.update_bundle(bundle, bundle_update, connection)

        return True

    def transition_bundle_running(self, bundle, worker_run, row, user_id, worker_id, connection):
        """
        Transitions bundle to RUNNING state:
            If bundle was WORKER_OFFLINE, also inserts a row into worker_run.
        """
        if row.state == State.WORKER_OFFLINE:
            run_row = connection.execute(
                cl_worker_run.select().where(cl_worker_run.c.run_uuid == bundle.uuid)
            ).fetchone()
            if run_row:
                # we should never get to this point: panic
                raise IntegrityError(
                    'worker_run row exists for a bundle in WORKER_OFFLINE state, uuid %s'
                    % (bundle.uuid,)
                )

            worker_run_row = {'user_id': user_id, 'worker_id': worker_id, 'run_uuid': bundle.uuid}
            connection.execute(cl_worker_run.insert().values(worker_run_row))

        metadata_update = {
            'run_status': worker_run.run_status,
            'last_updated': int(time.time()),
            'time': worker_run.container_time_total,
            'time_user': worker_run.container_time_user,
            'time_system': worker_run.container_time_system,
            'remote': worker_run.remote,
        }

        if worker_run.docker_image is not None:
            metadata_update['docker_image'] = worker_run.docker_image

        self.update_bundle(
            bundle, {'state': worker_run.state, 'metadata': metadata_update}, connection
        )

        return True

    def transition_bundle_worker_offline(self, bundle):
        """
        Transitions bundle to WORKER_OFFLINE state:
            Updates the last_updated metadata.
            Removes the corresponding row from worker_run if it exists.
        """
        with self.engine.begin() as connection:
            # Check that it still exists and is running
            row = connection.execute(
                cl_bundle.select().where(
                    cl_bundle.c.id == bundle.id
                    and (cl_bundle.c.state == State.RUNNING or cl_bundle.c.state == State.PREPARING)
                )
            ).fetchone()
            if not row:
                # The user deleted the bundle or the bundle finished
                return False

            # Delete row in worker_run
            connection.execute(
                cl_worker_run.delete().where(cl_worker_run.c.run_uuid == bundle.uuid)
            )

            bundle_update = {
                'state': State.WORKER_OFFLINE,
                'metadata': {'last_updated': int(time.time())},
            }
            self.update_bundle(bundle, bundle_update, connection)
        return True

    def transition_bundle_finalizing(self, bundle, worker_run, connection):
        """
        Transitions bundle to FINALIZING state:
            Saves the failure message and exit code from the worker
            If the user running the bundle was the CodaLab root user,
            increments the time used by the bundle owner.
        """
        failure_message, exitcode = worker_run.failure_message, worker_run.exitcode
        if failure_message is None and exitcode is not None and exitcode != 0:
            failure_message = 'Exit code %d' % exitcode
        # Build metadata
        metadata = {}
        if failure_message is not None:
            metadata['failure_message'] = failure_message
        if exitcode is not None:
            metadata['exitcode'] = exitcode

        bundle_update = {'state': State.FINALIZING, 'metadata': metadata}

        self.update_bundle(bundle, bundle_update, connection)
        return True

    def transition_bundle_finished(self, bundle, bundle_location):
        """
        Transitions bundle to READY or FAILED state:
            The final state is determined by whether a failure message or exitcode
            was recorded during finalization of the bundle.
        """
        metadata = bundle.metadata.to_dict()
        failure_message = metadata.get('failure_message', None)
        exitcode = metadata.get('exitcode', 0)
        state = State.FAILED if failure_message or exitcode else State.READY
        if failure_message == 'Kill requested':
            state = State.KILLED

        worker = self.get_bundle_worker(bundle.uuid)

        # Increment the amount of time used for the user whose bundles run on CodaLab's public instances
        if worker['user_id'] == self.root_user_id:
            self.increment_user_time_used(bundle.owner_id, metadata.get('time', 0))

        if worker['shared_file_system']:
            # TODO(Ashwin): fix for --link.
            self.update_disk_metadata(bundle, bundle_location)

        metadata = {'run_status': 'Finished', 'last_updated': int(time.time())}

        with self.engine.begin() as connection:
            self.update_bundle(bundle, {'state': state, 'metadata': metadata}, connection)
            connection.execute(
                cl_worker_run.delete().where(cl_worker_run.c.run_uuid == bundle.uuid)
            )

    # ==========================================================================
    # Bundle state machine helper functions
    # ==========================================================================

    def update_disk_metadata(self, bundle, bundle_location, enforce_disk_quota=False):
        """
        Computes the disk use and data hash of the given bundle.
        Updates the database rows for the bundle and user with the new disk use
        """
        dirs_and_files = None
        if os.path.isdir(bundle_location):
            dirs_and_files = path_util.recursive_ls(bundle_location)
        else:
            dirs_and_files = [], [bundle_location]

        # TODO(Ashwin): make this non-fs specific
        data_hash = '0x%s' % (path_util.hash_directory(bundle_location, dirs_and_files))
        data_size = path_util.get_size(bundle_location, dirs_and_files)
        if enforce_disk_quota:
            disk_left = self.get_user_disk_quota_left(bundle.owner_id)
            if data_size > disk_left:
                raise UsageError(
                    "Can't save bundle, bundle size %s greater than user's disk quota left: %s"
                    % (data_size, disk_left)
                )

        bundle_update = {'data_hash': data_hash, 'metadata': {'data_size': data_size}}
        self.update_bundle(bundle, bundle_update)
        self.update_user_disk_used(bundle.owner_id)

    def bundle_checkin(self, bundle, worker_run, user_id, worker_id):
        """
        Updates the database tables with the most recent bundle information from worker
        """
        with self.engine.begin() as connection:
            # If bundle isn't in db anymore the user deleted it so cancel
            row = connection.execute(
                cl_bundle.select().where(cl_bundle.c.id == bundle.id)
            ).fetchone()
            if not row:
                return False

            # Get staged bundle from worker checkin and move it to staged state
            if worker_run.state == State.STAGED:
                return self.transition_bundle_staged(bundle)

            if worker_run.state == State.FINALIZING:
                # update bundle metadata using transition_bundle_running one last time before finalizing it
                self.transition_bundle_running(
                    bundle, worker_run, row, user_id, worker_id, connection
                )
                return self.transition_bundle_finalizing(bundle, worker_run, connection)

            if worker_run.state in [State.PREPARING, State.RUNNING]:
                return self.transition_bundle_running(
                    bundle, worker_run, row, user_id, worker_id, connection
                )

            # State isn't one we can check in for
            return False

    def save_bundle(self, bundle):
        """
        Save a bundle. On success, sets the Bundle object's id from the result.
        """
        bundle.validate()
        bundle_value = bundle.to_dict(strict=False)
        dependency_values = bundle_value.pop('dependencies')
        metadata_values = bundle_value.pop('metadata')

        # Raises exception when the UUID uniqueness constraint is violated
        # (Clients should check for this case ahead of time if they want to
        # silently skip over creating bundles that already exist.)
        with self.engine.begin() as connection:
            result = connection.execute(cl_bundle.insert().values(bundle_value))
            self.do_multirow_insert(connection, cl_bundle_dependency, dependency_values)
            self.do_multirow_insert(connection, cl_bundle_metadata, metadata_values)
            bundle.id = result.lastrowid

    def update_bundle(self, bundle, update, connection=None, delete=False):
        """
        For each key-value pair in the update dictionary, add or update key-value pair. Note
        that metadata keys not in the update dictionary are not affected in the update operation.
        Also, delete any metadata key-value pairs when the value specified is None.
        This method validates all updates to the bundle, so it is appropriate
        to use this method to update bundles based on user input (eg: cl edit).
        """
        message = 'Illegal update: %s' % (update,)
        precondition('id' not in update and 'uuid' not in update, message)
        # Apply the column and metadata updates in memory and validate the result.
        metadata_update = update.pop('metadata', {})
        bundle.update_in_memory(update)

        # Generate a list of metadata keys that will be deleted and update metadata key-value pair
        metadata_delete_keys = []
        for key, value in metadata_update.items():
            # Delete the key,value pair when the following two conditions are met:
            # 1. the delete flag is True
            # 2. the value is None
            if delete and value is None:
                bundle.metadata.remove_metadata_key(key)
                metadata_delete_keys.append(key)
            else:
                bundle.metadata.set_metadata_key(key, value)

        # Delete metadata keys from metadata_update dictionary
        for key in metadata_delete_keys:
            del metadata_update[key]

        bundle.validate()
        # Construct clauses and update lists for updating certain bundle columns.
        if update:
            clause = cl_bundle.c.uuid == bundle.uuid
        if metadata_update:
            metadata_update_clause = and_(
                cl_bundle_metadata.c.bundle_uuid == bundle.uuid,
                cl_bundle_metadata.c.metadata_key.in_(metadata_update),
            )
            metadata_update_values = [
                row_dict
                for row_dict in bundle.to_dict().pop('metadata')
                if row_dict['metadata_key'] in metadata_update
            ]
        if metadata_delete_keys:
            metadata_delete_clause = and_(
                cl_bundle_metadata.c.bundle_uuid == bundle.uuid,
                cl_bundle_metadata.c.metadata_key.in_(metadata_delete_keys),
            )

        # Perform the actual updates and deletes.
        def do_update(connection):
            try:
                if update:
                    connection.execute(cl_bundle.update().where(clause).values(update))
                if metadata_update:
                    connection.execute(cl_bundle_metadata.delete().where(metadata_update_clause))
                    self.do_multirow_insert(connection, cl_bundle_metadata, metadata_update_values)
                if metadata_delete_keys:
                    connection.execute(cl_bundle_metadata.delete().where(metadata_delete_clause))
            except UnicodeError:
                raise UsageError("Invalid character detected; use ascii characters only.")

        if connection:
            do_update(connection)
        else:
            with self.engine.begin() as connection:
                do_update(connection)

    def get_bundle_dependencies(self, uuid):
        with self.engine.begin() as connection:
            dependency_rows = connection.execute(
                cl_bundle_dependency.select()
                .where(cl_bundle_dependency.c.child_uuid == uuid)
                .order_by(cl_bundle_dependency.c.id)
            ).fetchall()
        return [Dependency(dep_val) for dep_val in dependency_rows]

    def get_bundle_state(self, uuid):
        result_dict = self.get_bundle_states([uuid])
        if uuid not in result_dict:
            raise NotFoundError('Could not find bundle with uuid %s' % uuid)
        return result_dict[uuid]

    def get_bundle_states(self, uuids):
        """
        Return {uuid: state, ...}
        """
        with self.engine.begin() as connection:
            rows = connection.execute(
                select([cl_bundle.c.uuid, cl_bundle.c.state]).where(cl_bundle.c.uuid.in_(uuids))
            ).fetchall()
            return dict((r.uuid, r.state) for r in rows)

    def delete_bundles(self, uuids):
        """
        Delete bundles with the given uuids.
        """
        with self.engine.begin() as connection:
            # We must delete bundles rows in the opposite order that we create them
            # to avoid foreign-key constraint failures.
            connection.execute(
                cl_group_bundle_permission.delete().where(
                    cl_group_bundle_permission.c.object_uuid.in_(uuids)
                )
            )
            connection.execute(
                cl_worksheet_item.delete().where(cl_worksheet_item.c.bundle_uuid.in_(uuids))
            )
            connection.execute(
                cl_bundle_metadata.delete().where(cl_bundle_metadata.c.bundle_uuid.in_(uuids))
            )
            connection.execute(
                cl_bundle_dependency.delete().where(cl_bundle_dependency.c.child_uuid.in_(uuids))
            )
            # In case something goes wrong, delete bundles that are currently running on workers.
            connection.execute(cl_worker_run.delete().where(cl_worker_run.c.run_uuid.in_(uuids)))
            connection.execute(cl_bundle.delete().where(cl_bundle.c.uuid.in_(uuids)))

    def remove_data_hash_references(self, uuids):
        with self.engine.begin() as connection:
            connection.execute(
                cl_bundle.update().where(cl_bundle.c.uuid.in_(uuids)).values({'data_hash': None})
            )

    # ==========================================================================
    # Worksheet-related model methods follow!
    # ==========================================================================

    def get_worksheet(self, uuid, fetch_items):
        """
        Get a worksheet given its uuid.
        :rtype: Worksheet
        """
        worksheets = self.batch_get_worksheets(fetch_items=fetch_items, uuid=uuid)
        if not worksheets:
            raise NotFoundError('Could not find worksheet with uuid %s' % (uuid,))
        if len(worksheets) > 1:
            raise IntegrityError('Found multiple worksheets with uuid %s' % (uuid,))
        return worksheets[0]

    def batch_get_worksheets(self, fetch_items, **kwargs):
        """
        Get a list of worksheets, all of which satisfy the clause given by kwargs.
        :rtype: list[Worksheet]
        """
        base_worksheet_uuid = kwargs.pop('base_worksheet_uuid', None)
        clause = self.make_kwargs_clause(cl_worksheet, kwargs)
        # Handle base_worksheet_uuid specially
        if base_worksheet_uuid:
            clause = and_(
                clause,
                cl_worksheet_item.c.subworksheet_uuid == cl_worksheet.c.uuid,
                cl_worksheet_item.c.worksheet_uuid == base_worksheet_uuid,
            )

        with self.engine.begin() as connection:
            worksheet_rows = connection.execute(
                cl_worksheet.select().distinct().where(clause)
            ).fetchall()
            if not worksheet_rows:
                if base_worksheet_uuid is not None:
                    # We didn't find any results restricting to base_worksheet_uuid,
                    # so do a global search
                    return self.batch_get_worksheets(fetch_items, **kwargs)
                return []
            # Get the tags
            uuids = set(row.uuid for row in worksheet_rows)
            tag_rows = connection.execute(
                cl_worksheet_tag.select().where(cl_worksheet_tag.c.worksheet_uuid.in_(uuids))
            ).fetchall()
            # Fetch the items of all the worksheets
            if fetch_items:
                item_rows = connection.execute(
                    cl_worksheet_item.select().where(cl_worksheet_item.c.worksheet_uuid.in_(uuids))
                ).fetchall()

        # Make a dictionary for each worksheet with both its main row and its items.
        worksheet_values = {row.uuid: str_key_dict(row) for row in worksheet_rows}
        # Set tags
        for value in worksheet_values.values():
            value['tags'] = []
            if value['title']:
                value['title'] = self.decode_str(value['title'])
        for row in tag_rows:
            worksheet_values[row.worksheet_uuid]['tags'].append(row.tag)
        if fetch_items:
            for value in worksheet_values.values():
                value['items'] = []
            for item_row in sorted(item_rows, key=item_sort_key):
                if item_row.worksheet_uuid not in worksheet_values:
                    raise IntegrityError('Got item %s without worksheet' % (item_row,))
                item_row = dict(item_row)
                item_row['value'] = self.decode_str(item_row['value'])
                worksheet_values[item_row['worksheet_uuid']]['items'].append(item_row)
        return [Worksheet(value) for value in worksheet_values.values()]

    def search_worksheets(self, user_id, keywords):
        """
        Return a list of row dicts, one per worksheet. These dicts do NOT contain
        ALL worksheet items; this method is meant to make it easy for a user to see
        their existing worksheets.
        Note: keywords has basically same semantics as search_bundles.
        """
        clauses = []
        offset = 0
        limit = SEARCH_RESULTS_LIMIT
        sort_key = [cl_worksheet.c.date_last_modified.desc(), cl_worksheet.c.name]

        # Number nested subqueries
        subquery_index = [0]

        def alias(clause):
            subquery_index[0] += 1
            return clause.alias('q' + str(subquery_index[0]))

        def make_condition(field, value):
            # Special
            if value == '.sort':
                sort_key[0] = field
            elif value == '.sort-':
                sort_key[0] = desc(field)
            else:
                # Ordinary value
                if isinstance(value, list):
                    return field.in_(value)
                if '%' in value:
                    return field.like(value)
                return field == value
            return None

        for keyword in keywords:
            keyword = keyword.replace('.*', '%')
            # Sugar
            if keyword == '.mine':
                keyword = 'owner_id=' + (user_id or '')
            elif keyword == '.last':
                keyword = 'id=.sort-'
            elif keyword == '.shared':
                keyword = '.shared=True'

            m = SEARCH_KEYWORD_REGEX.match(keyword)  # key=value
            if m:
                key, value = m.group(1), m.group(2)
                if ',' in value:  # value is value1,value2
                    value = value.split(',')
            else:
                key, value = 'uuid_name_title', keyword

            clause = None
            # Special functions
            if key == '.offset':
                offset = int(value)
            elif key == '.limit':
                limit = int(value)
            elif key == '.shared':  # shared with any group I am in with read or all permission?
                clause = cl_worksheet.c.uuid.in_(
                    select([cl_group_worksheet_permission.c.object_uuid]).where(
                        and_(
                            cl_group_worksheet_permission.c.group_uuid.in_(
                                alias(
                                    select([cl_user_group.c.group_uuid]).where(
                                        cl_user_group.c.user_id == user_id
                                    )
                                )
                            ),
                            cl_group_worksheet_permission.c.permission
                            >= GROUP_OBJECT_PERMISSION_READ,
                        )
                    )
                )
            # Bundle fields
            elif key in ('id', 'uuid', 'name', 'title', 'owner_id'):
                clause = make_condition(getattr(cl_worksheet.c, key), value)
            elif key == 'group':  # shared with group with read or all permissions?
                group_uuid = get_group_info(value, False)['uuid']
                clause = cl_worksheet.c.uuid.in_(
                    select([cl_group_worksheet_permission.c.object_uuid]).where(
                        and_(
                            cl_group_worksheet_permission.c.group_uuid == group_uuid,
                            cl_group_worksheet_permission.c.permission
                            >= GROUP_OBJECT_PERMISSION_READ,
                        )
                    )
                )
            elif key == 'bundle':  # contains bundle?
                condition = make_condition(cl_worksheet_item.c.bundle_uuid, value)
                if condition is None:  # top-level
                    clause = (
                        cl_worksheet_item.c.worksheet_uuid == cl_worksheet.c.uuid
                    )  # Join constraint
                else:
                    clause = cl_worksheet.c.uuid.in_(
                        alias(select([cl_worksheet_item.c.worksheet_uuid]).where(condition))
                    )
            elif key == 'worksheet':  # contains worksheet?
                condition = make_condition(cl_worksheet_item.c.subworksheet_uuid, value)
                if condition is None:  # top-level
                    clause = (
                        cl_worksheet_item.c.worksheet_uuid == cl_worksheet.c.uuid
                    )  # Join constraint
                else:
                    clause = cl_worksheet.c.uuid.in_(
                        alias(select([cl_worksheet_item.c.worksheet_uuid]).where(condition))
                    )
            elif key == 'tag':  # has tag?
                condition = make_condition(cl_worksheet_tag.c.tag, value)
                if condition is None:  # top-level
                    clause = (
                        cl_worksheet_tag.c.worksheet_uuid == cl_worksheet.c.uuid
                    )  # Join constraint
                else:
                    clause = cl_worksheet.c.uuid.in_(
                        alias(select([cl_worksheet_tag.c.worksheet_uuid]).where(condition))
                    )
            elif key == 'uuid_name_title':  # Search uuid and name by default
                clause = or_(
                    cl_worksheet.c.uuid.like('%' + value + '%'),
                    cl_worksheet.c.name.like('%' + value + '%'),
                    cl_worksheet.c.title.like('%' + value + '%'),
                )
            elif key == '':  # Match any field
                clause = []
                clause.append(cl_worksheet.c.uuid.like('%' + value + '%'))
                clause.append(cl_worksheet.c.name.like('%' + value + '%'))
                clause.append(cl_worksheet.c.title.like('%' + value + '%'))
                clause.append(
                    cl_worksheet.c.uuid.in_(
                        alias(
                            select([cl_worksheet_item.c.worksheet_uuid]).where(
                                cl_worksheet_item.c.value.like('%' + value + '%')
                            )
                        )
                    )
                )
                clause = or_(*clause)
            else:
                raise UsageError('Unknown key: %s' % key)

            if clause is not None:
                clauses.append(clause)

        clause = and_(*clauses)

        # Enforce permissions
        if user_id != self.root_user_id:
            access_via_owner = cl_worksheet.c.owner_id == user_id
            access_via_group = cl_worksheet.c.uuid.in_(
                select([cl_group_worksheet_permission.c.object_uuid]).where(
                    or_(
                        cl_group_worksheet_permission.c.group_uuid
                        == self.public_group_uuid,  # Public group
                        cl_group_worksheet_permission.c.group_uuid.in_(  # Private group
                            alias(
                                select([cl_user_group.c.group_uuid]).where(
                                    cl_user_group.c.user_id == user_id
                                )
                            )
                        ),
                    )
                )
            )
            clause = and_(clause, or_(access_via_owner, access_via_group))

        cols_to_select = [
            cl_worksheet.c.id,
            cl_worksheet.c.uuid,
            cl_worksheet.c.name,
            cl_worksheet.c.title,
            cl_worksheet.c.frozen,
            cl_worksheet.c.owner_id,
            cl_worksheet.c.date_last_modified,
        ]
        query = (
            select(cols_to_select)
            .distinct()
            .where(clause)
            .offset(offset)
            .order_by(desc(cl_worksheet.c.owner_id == user_id))
            .limit(limit)
        )

        # Sort
        if sort_key[0] is not None:
            query = query.order_by(*sort_key)

        with self.engine.begin() as connection:
            rows = connection.execute(query).fetchall()
            if not rows:
                return []

        # Get permissions of the worksheets
        worksheet_uuids = [row.uuid for row in rows]
        uuid_group_permissions = self.batch_get_group_worksheet_permissions(
            user_id, worksheet_uuids
        )

        # Put the permissions into the worksheets
        row_dicts = []
        for row in rows:
            row = str_key_dict(row)
            row['group_permissions'] = uuid_group_permissions[row['uuid']]
            if row['title']:
                row['title'] = self.decode_str(row['title'])
            row_dicts.append(row)
        return row_dicts

    def new_worksheet(self, worksheet):
        """
        Save the given (empty) worksheet to the database. On success, set its id.
        """
        now = datetime.datetime.utcnow()
        message = 'save_worksheet called with non-empty worksheet: %s' % (worksheet,)
        precondition(not worksheet.items, message)
        worksheet.validate()
        worksheet_value = worksheet.to_dict()
        worksheet_value.pop('tags')
        worksheet_value.pop('items')
        worksheet_value.pop('last_item_id')
        worksheet_value['date_created'] = now
        worksheet_value['date_last_modified'] = now
        with self.engine.begin() as connection:
            result = connection.execute(cl_worksheet.insert().values(worksheet_value))
            worksheet.id = result.lastrowid

    def add_worksheet_items(self, worksheet_uuid, items, after_sort_key=None, replace=[]):
        """
        Add worksheet items *items* to the position *after_sort_key* to the worksheet,
        removing items specified by *replace* if necessary.
        """
        with self.engine.begin() as connection:
            if len(replace) > 0:
                # Remove the old items.
                connection.execute(
                    cl_worksheet_item.delete().where(cl_worksheet_item.c.id.in_(replace))
                )
            if len(items) == 0:
                # Nothing to insert, return
                return
            if after_sort_key is not None:
                after_sort_key = int(after_sort_key)
                # Shift existing items' sort_keys for items that originally came after
                # the after_sort_key
                offset = len(items)
                clause = and_(
                    cl_worksheet_item.c.worksheet_uuid == worksheet_uuid,
                    or_(
                        cl_worksheet_item.c.sort_key > after_sort_key,
                        and_(
                            cl_worksheet_item.c.sort_key is None,
                            cl_worksheet_item.c.id > after_sort_key,
                        ),
                    ),
                )
                query = select(['*']).where(clause)
                # Get result in a list
                after_items = [item for item in connection.execute(query)]
                if (
                    len(after_items) > 0
                    and min(item_sort_key(item) for item in after_items) - after_sort_key <= offset
                ):
                    # Shift the keys of the original items if the gap between after_sort_key
                    # and the next smallest key is not sufficient for inserting items.
                    # In actuality, delete these items and re-insert.
                    connection.execute(cl_worksheet_item.delete().where(clause))
                    new_after_items = [
                        {
                            'worksheet_uuid': item.worksheet_uuid,
                            'bundle_uuid': item.bundle_uuid,
                            'subworksheet_uuid': item.subworksheet_uuid,
                            'value': item.value,
                            'type': item.type,
                            'sort_key': item_sort_key(item) + offset,
                        }
                        for item in after_items
                    ]
                    self.do_multirow_insert(connection, cl_worksheet_item, new_after_items)
            # Insert new items
            items_to_insert = [
                {
                    'worksheet_uuid': worksheet_uuid,
                    'bundle_uuid': bundle_uuid,
                    'subworksheet_uuid': subworksheet_uuid,
                    'value': self.encode_str(value),
                    'type': type,
                    'sort_key': after_sort_key + idx + 1 if after_sort_key is not None else None,
                }
                for idx, (bundle_uuid, subworksheet_uuid, value, type) in enumerate(items)
            ]
            self.do_multirow_insert(connection, cl_worksheet_item, items_to_insert)
        self.update_worksheet_last_modified_date(worksheet_uuid)

    def add_shadow_worksheet_items(self, old_bundle_uuid, new_bundle_uuid):
        """
        For each occurrence of old_bundle_uuid in any worksheet, add
        new_bundle_uuid right after it (a shadow).
        """
        with self.engine.begin() as connection:
            # Find all the worksheet_items that old_bundle_uuid appears in
            query = select(
                [cl_worksheet_item.c.worksheet_uuid, cl_worksheet_item.c.sort_key]
            ).where(cl_worksheet_item.c.bundle_uuid == old_bundle_uuid)
            old_items = connection.execute(query)

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

    def update_worksheet_item_value(self, id, value):
        """
        Update the value of a worksheet item, aka updating a markdown item.
        When the value is falsy, delete this item.
        """
        with self.engine.begin() as connection:
            if value:
                connection.execute(
                    cl_worksheet_item.update()
                    .where(cl_worksheet_item.c.id == id)
                    .values({'value': value})
                )
            else:
                connection.execute(cl_worksheet_item.delete().where(cl_worksheet_item.c.id == id))

    def update_worksheet_items(self, worksheet_uuid, last_item_id, length, new_items):
        """
        Updates the worksheet with the given uuid. If there were exactly
        `last_length` items with database id less than `last_id`, replaces them all
        with the items in new_items. Does NOT affect items in this worksheet with
        database id greater than last_id.

        Does NOT affect items that were added to the worksheet in between the
        time it was retrieved and it was updated.

        If this worksheet were updated between the time it was retrieved and
        updated, this method will raise a UsageError.
        """
        clause = and_(
            cl_worksheet_item.c.worksheet_uuid == worksheet_uuid,
            cl_worksheet_item.c.id <= last_item_id,
        )
        # See codalab.objects.worksheet for an explanation of the sort_key protocol.
        # We need to produce sort keys here that are strictly upper-bounded by the
        # last known item id in this worksheet, and which monotonically increase.
        # The expression last_item_id + i - len(new_items) works. It can produce
        # negative sort keys, but that's fine.
        new_item_values = [
            {
                'worksheet_uuid': worksheet_uuid,
                'bundle_uuid': bundle_uuid,
                'subworksheet_uuid': subworksheet_uuid,
                'value': self.encode_str(value),
                'type': item_type,
                'sort_key': (last_item_id + i - len(new_items)),
            }
            for (i, (bundle_uuid, subworksheet_uuid, value, item_type)) in enumerate(new_items)
        ]
        with self.engine.begin() as connection:
            result = connection.execute(cl_worksheet_item.delete().where(clause))
            message = 'Found extra items for worksheet %s' % (worksheet_uuid,)
            precondition(result.rowcount <= length, message)
            if result.rowcount < length:
                raise UsageError('Worksheet %s was updated concurrently!' % (worksheet_uuid,))
            self.do_multirow_insert(connection, cl_worksheet_item, new_item_values)
        self.update_worksheet_last_modified_date(worksheet_uuid)

    def update_worksheet_last_modified_date(self, worksheet_id):
        """
        Update worksheet's last modified date to now.
        Calling update_worksheet_metadata with an empty argument is equivalent to updating the last modified date.
        """
        worksheet = self.get_worksheet(worksheet_id, fetch_items=False)
        self.update_worksheet_metadata(worksheet, {})

    def update_worksheet_metadata(self, worksheet, info):
        """
        Update the given worksheet's metadata.
        """
        if 'name' in info:
            worksheet.name = info['name']
        if 'frozen' in info:
            worksheet.frozen = info['frozen']
        if 'owner_id' in info:
            worksheet.owner_id = info['owner_id']
        if 'title' in info:
            info['title'] = self.encode_str(info['title'])
        # Always update worksheet's last modified date to current timestamp (UTC)
        info['date_last_modified'] = datetime.datetime.utcnow()
        worksheet.date_last_modified = info['date_last_modified']

        worksheet.validate()
        with self.engine.begin() as connection:
            if 'tags' in info:
                # Delete old tags
                connection.execute(
                    cl_worksheet_tag.delete().where(
                        cl_worksheet_tag.c.worksheet_uuid == worksheet.uuid
                    )
                )
                # Add new tags
                new_tag_values = [
                    {'worksheet_uuid': worksheet.uuid, 'tag': tag} for tag in info['tags']
                ]
                self.do_multirow_insert(connection, cl_worksheet_tag, new_tag_values)
                del info['tags']
            if len(info) > 0:
                connection.execute(
                    cl_worksheet.update().where(cl_worksheet.c.uuid == worksheet.uuid).values(info)
                )

    def delete_worksheet(self, worksheet_uuid):
        """
        Delete the worksheet with the given uuid.
        """
        with self.engine.begin() as connection:
            connection.execute(
                cl_group_worksheet_permission.delete().where(
                    cl_group_worksheet_permission.c.object_uuid == worksheet_uuid
                )
            )
            connection.execute(
                cl_worksheet_item.delete().where(
                    cl_worksheet_item.c.worksheet_uuid == worksheet_uuid
                )
            )
            connection.execute(
                cl_worksheet_item.delete().where(
                    cl_worksheet_item.c.subworksheet_uuid == worksheet_uuid
                )
            )
            connection.execute(
                cl_worksheet_tag.delete().where(cl_worksheet_tag.c.worksheet_uuid == worksheet_uuid)
            )
            connection.execute(cl_worksheet.delete().where(cl_worksheet.c.uuid == worksheet_uuid))

    # ===========================================================================
    # Group and permission-related methods
    # ===========================================================================

    def _create_default_groups(self):
        """
        Create system-defined groups. This is called by create_tables.
        """
        groups = self.batch_get_groups(name='public', user_defined=False)
        if len(groups) == 0:
            group_dict = self.create_group(
                {
                    'uuid': spec_util.generate_uuid(),
                    'name': 'public',
                    'owner_id': None,
                    'user_defined': False,
                }
            )
        else:
            group_dict = groups[0]
        self.public_group_uuid = group_dict['uuid']

    def create_group(self, group_dict):
        """
        Create the group specified by the given row dict.
        """
        with self.engine.begin() as connection:
            group_dict['uuid'] = spec_util.generate_uuid()
            result = connection.execute(cl_group.insert().values(group_dict))
            group_dict['id'] = result.lastrowid
        return group_dict

    def batch_get_groups(self, **kwargs):
        """
        Get a list of groups, all of which satisfy the clause given by kwargs.
        """
        clause = self.make_kwargs_clause(cl_group, kwargs)
        with self.engine.begin() as connection:
            rows = connection.execute(cl_group.select().where(clause)).fetchall()
            if not rows:
                return []
        values = {row.uuid: str_key_dict(row) for row in rows}
        return [value for value in values.values()]

    def batch_get_all_groups(self, spec_filters, group_filters, user_group_filters):
        """
        Get a list of groups by querying the group table and/or the user_group table.
        Take the union of the two results.  This method performs the general query:
        - q0: use spec_filters on the public group
        - q1: use spec_filters and group_filters on group
        - q2: use spec_filters and user_group_filters on user_group
        return union(q0, q1, q2)
        """
        fetch_cols = [cl_group.c.uuid, cl_group.c.name, cl_group.c.owner_id]
        fetch_cols0 = fetch_cols + [
            cl_group.c.owner_id.label('user_id'),
            literal(False).label('is_admin'),
        ]
        fetch_cols1 = fetch_cols + [
            cl_group.c.owner_id.label('user_id'),
            literal(True).label('is_admin'),
        ]
        fetch_cols2 = fetch_cols + [cl_user_group.c.user_id, cl_user_group.c.is_admin]

        q0 = None
        q1 = None
        q2 = None

        if spec_filters:
            spec_clause = self.make_kwargs_clause(cl_group, spec_filters)
            q0 = select(fetch_cols0).where(spec_clause)
            q1 = select(fetch_cols1).where(spec_clause)
            q2 = (
                select(fetch_cols2)
                .where(spec_clause)
                .where(cl_group.c.uuid == cl_user_group.c.group_uuid)
            )
        if q0 is None:
            q0 = select(fetch_cols0)
        q0 = q0.where(cl_group.c.uuid == self.public_group_uuid)
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

        # Union
        q0 = union(*[q for q in [q0, q1, q2] if q is not None])

        with self.engine.begin() as connection:
            rows = connection.execute(q0).fetchall()
            if not rows:
                return []
            for i, row in enumerate(rows):
                row = str_key_dict(row)
                # TODO: remove these conversions once database schema is changed from int to str
                if isinstance(row['user_id'], int):
                    row['user_id'] = str(row['user_id'])
                if isinstance(row['owner_id'], int):
                    row['owner_id'] = str(row['owner_id'])
                rows[i] = row
            values = {row['uuid']: row for row in rows}
            return [value for value in values.values()]

    def delete_group(self, uuid):
        """
        Delete the group with the given uuid.
        """
        with self.engine.begin() as connection:
            connection.execute(
                cl_group_bundle_permission.delete().where(
                    cl_group_bundle_permission.c.group_uuid == uuid
                )
            )
            connection.execute(
                cl_group_worksheet_permission.delete().where(
                    cl_group_worksheet_permission.c.group_uuid == uuid
                )
            )
            connection.execute(cl_user_group.delete().where(cl_user_group.c.group_uuid == uuid))
            connection.execute(cl_group.delete().where(cl_group.c.uuid == uuid))

    def add_user_in_group(self, user_id, group_uuid, is_admin):
        """
        Add user as a member of a group.
        """
        row = {'group_uuid': group_uuid, 'user_id': user_id, 'is_admin': is_admin}
        with self.engine.begin() as connection:
            result = connection.execute(cl_user_group.insert().values(row))
            row['id'] = result.lastrowid
        return row

    def delete_user_in_group(self, user_id, group_uuid):
        """
        Add user as a member of a group.
        """
        with self.engine.begin() as connection:
            connection.execute(
                cl_user_group.delete()
                .where(cl_user_group.c.user_id == user_id)
                .where(cl_user_group.c.group_uuid == group_uuid)
            )

    def update_user_in_group(self, user_id, group_uuid, is_admin):
        """
        Update user role in group.
        """
        with self.engine.begin() as connection:
            connection.execute(
                cl_user_group.update()
                .where(cl_user_group.c.user_id == user_id)
                .where(cl_user_group.c.group_uuid == group_uuid)
                .values({'is_admin': is_admin})
            )

    def batch_get_user_in_group(self, **kwargs):
        """
        Return list of user-group entries matching the specified |kwargs|.
        Can be used to get groups for a user or users in a group.
        Examples: user_id=..., group_uuid=...
        """
        clause = self.make_kwargs_clause(cl_user_group, kwargs)
        with self.engine.begin() as connection:
            rows = connection.execute(cl_user_group.select().where(clause)).fetchall()
            if not rows:
                return []
        return [str_key_dict(row) for row in rows]

    def get_user_groups(self, user_id):
        """
        Get the list of groups that the user belongs to
        :param user_id: ID of the user
        :return: A list of group uuid's
        """
        groups = [self.public_group_uuid]  # Everyone is in the public group implicitly.
        if user_id is not None:
            groups += [row['group_uuid'] for row in self.batch_get_user_in_group(user_id=user_id)]
        return groups

    def set_group_permission(self, table, group_uuid, object_uuid, new_permission):
        """
        Atomically set group permission on object. Does NOT check for user
        permissions on the bundle or the group.

        :param table: cl_group_bundle_permission or cl_group_worksheet_permission
        :param group_uuid: uuid of group for which to set permission
        :param object_uuid: uuid of object (bundle or worksheet) on which to set permission
        :param new_permission: new permission integer
        """
        with self.engine.begin() as connection:
            row = connection.execute(
                table.select()
                .where(and_(table.c.object_uuid == object_uuid, table.c.group_uuid == group_uuid))
                .limit(1)
            ).fetchone()
            old_permission = row.permission if row else GROUP_OBJECT_PERMISSION_NONE

            if new_permission > 0:
                if old_permission > 0:
                    # Update existing permission
                    connection.execute(
                        table.update()
                        .where(table.c.group_uuid == group_uuid)
                        .where(table.c.object_uuid == object_uuid)
                        .values({'permission': new_permission})
                    )
                else:
                    # Create permission
                    connection.execute(
                        table.insert().values(
                            {
                                'group_uuid': group_uuid,
                                'object_uuid': object_uuid,
                                'permission': new_permission,
                            }
                        )
                    )
            else:
                if old_permission > 0:
                    # Delete permission
                    connection.execute(
                        table.delete()
                        .where(table.c.group_uuid == group_uuid)
                        .where(table.c.object_uuid == object_uuid)
                    )

    def set_group_bundle_permission(self, group_uuid, bundle_uuid, new_permission):
        return self.set_group_permission(
            cl_group_bundle_permission, group_uuid, bundle_uuid, new_permission
        )

    def set_group_worksheet_permission(self, group_uuid, worksheet_uuid, new_permission):
        return self.set_group_permission(
            cl_group_worksheet_permission, group_uuid, worksheet_uuid, new_permission
        )

    def batch_get_group_permissions(self, table, user_id, object_uuids):
        """
        Return map from object_uuid to list of {group_uuid: ..., group_name: ..., permission: ...}
        Note: if user_id is not None, only involve groups that user_id is in. If user_id is None (i.e.
        user is not logged in), involve only the public group.
        """
        with self.engine.begin() as connection:
            if user_id is None:
                # Not logged in: include only public group
                group_restrict = table.c.group_uuid == self.public_group_uuid
            else:
                # Logged in as root: include all groups
                group_restrict = true()

            rows = connection.execute(
                select([table, cl_group.c.name])
                .where(table.c.group_uuid == cl_group.c.uuid)
                .where(group_restrict)
                .where(table.c.object_uuid.in_(object_uuids))
                .order_by(cl_group.c.name)
            ).fetchall()
            result = collections.defaultdict(list)  # object_uuid => list of rows
            for row in rows:
                result[row.object_uuid].append(
                    {
                        'id': row.id,
                        'group_uuid': row.group_uuid,
                        'group_name': row.name,
                        'permission': row.permission,
                    }
                )
            return result

    def batch_get_group_bundle_permissions(self, user_id, bundle_uuids):
        return self.batch_get_group_permissions(cl_group_bundle_permission, user_id, bundle_uuids)

    def batch_get_group_worksheet_permissions(self, user_id, worksheet_uuids):
        return self.batch_get_group_permissions(
            cl_group_worksheet_permission, user_id, worksheet_uuids
        )

    def get_group_permissions(self, table, user_id, object_uuid):
        """
        Return list of {group_uuid: ..., group_name: ..., permission: ...} entries for the given object.
        Restrict to groups that user_id is a part of.
        """
        return self.batch_get_group_permissions(table, user_id, [object_uuid])[object_uuid]

    def get_group_bundle_permissions(self, user_id, bundle_uuid):
        return self.get_group_permissions(cl_group_bundle_permission, user_id, bundle_uuid)

    def get_group_worksheet_permissions(self, user_id, worksheet_uuid):
        return self.get_group_permissions(cl_group_worksheet_permission, user_id, worksheet_uuid)

    def get_user_permissions(self, table, user_id, object_uuids, owner_ids):
        """
        Gets the set of permissions granted to the given user on the given objects.
        owner_ids: map from object_uuid to owner_id.
        Return: map from object_uuid to integer permission.

        Use user_id = None to check the set of permissions of an anonymous user.
        To compute this, look at the groups that the user belongs to.
        """
        object_permissions = dict(
            (object_uuid, GROUP_OBJECT_PERMISSION_NONE) for object_uuid in object_uuids
        )

        remaining_object_uuids = []
        for object_uuid in object_uuids:
            owner_id = owner_ids.get(object_uuid)
            # Owner and root has all permissions.
            if user_id == owner_id or user_id == self.root_user_id:
                object_permissions[object_uuid] = GROUP_OBJECT_PERMISSION_ALL
            else:
                remaining_object_uuids.append(object_uuid)

        if len(remaining_object_uuids) > 0:
            result = self.batch_get_group_permissions(table, user_id, remaining_object_uuids)
            user_groups = self.get_user_groups(user_id)
            for object_uuid, permissions in result.items():
                for row in permissions:
                    if row['group_uuid'] in user_groups:
                        object_permissions[object_uuid] = max(
                            object_permissions[object_uuid], row['permission']
                        )
        return object_permissions

    def get_user_bundle_permissions(self, user_id, bundle_uuids, owner_ids):
        return self.get_user_permissions(
            cl_group_bundle_permission, user_id, bundle_uuids, owner_ids
        )

    def get_user_worksheet_permissions(self, user_id, worksheet_uuids, owner_ids):
        return self.get_user_permissions(
            cl_group_worksheet_permission, user_id, worksheet_uuids, owner_ids
        )

    # Operations on the query log
    @staticmethod
    def date_handler(obj):
        """
        Helper function to serialize DataTime
        """
        return obj.isoformat() if isinstance(obj, (datetime.date, datetime.datetime)) else None

    def add_chat_log_info(self, query_info):
        """
        Add the given chat into the database
        Return a list of chats that the sender have had
        """
        sender_user_id = query_info.get('sender_user_id')
        recipient_user_id = query_info.get('recipient_user_id')
        message = query_info.get('message')
        worksheet_uuid = query_info.get('worksheet_uuid')
        bundle_uuid = query_info.get('bundle_uuid')
        with self.engine.begin() as connection:
            info = {
                'time': datetime.datetime.fromtimestamp(time.time()),
                'sender_user_id': sender_user_id,
                'recipient_user_id': recipient_user_id,
                'message': message,
                'worksheet_uuid': worksheet_uuid,
                'bundle_uuid': bundle_uuid,
            }
            connection.execute(cl_chat.insert().values(info))
        result = self.get_chat_log_info({'user_id': sender_user_id})
        return result

    def get_chat_log_info(self, query_info):
        """
        |query_info| specifies the user_id of the user that you are querying about.
        Example: query_info = {
            user_id: 2,   // get the chats sent by and received by the user with user_id 2
            limit: 20,   // get the most recent 20 chats related to this user. This is optional, as by default it will get all the chats.
        }
        Return a list of chats that the user have had given the user_id
        """
        user_id1 = query_info.get('user_id')
        if user_id1 is None:
            return None
        limit = query_info.get('limit')
        with self.engine.begin() as connection:
            query = select(
                [
                    cl_chat.c.time,
                    cl_chat.c.sender_user_id,
                    cl_chat.c.recipient_user_id,
                    cl_chat.c.message,
                ]
            )
            clause = []
            # query all chats that this user sends or receives
            clause.append(cl_chat.c.sender_user_id == user_id1)
            clause.append(cl_chat.c.recipient_user_id == user_id1)
            if user_id1 == self.root_user_id:
                # if this user is root user, also query all chats that system user sends or receives
                clause.append(cl_chat.c.sender_user_id == self.system_user_id)
                clause.append(cl_chat.c.recipient_user_id == self.system_user_id)
            clause = or_(*clause)
            query = query.where(clause)
            if limit is not None:
                query = query.limit(limit)
            # query = query.order_by(cl_chat.c.id.desc())
            rows = connection.execute(query).fetchall()
            result = [
                {
                    'message': row.message,
                    'time': row.time.strftime("%Y-%m-%d %H:%M:%S"),
                    'sender_user_id': row.sender_user_id,
                    'recipient_user_id': row.recipient_user_id,
                }
                for row in rows
            ]
            return result

    # ===========================================================================
    # User-related methods follow!
    # ===========================================================================

    def find_user(self, user_spec, check_active=True):
        user = self.get_user(user_id=user_spec, username=user_spec, check_active=check_active)
        if user is None:
            raise NotFoundError("User matching %r not found" % user_spec)
        return user

    def get_user(self, user_id=None, username=None, check_active=True):
        """
        Get user.

        :param user_id: user id of user to fetch
        :param username: username or email of user to fetch
        :return: User object, or None if no matching user.
        """
        user_ids = None
        usernames = None
        if user_id is not None:
            user_ids = [user_id]
        if username is not None:
            usernames = [username]
        result = self.get_users(
            user_ids=user_ids, usernames=usernames, check_active=check_active, limit=1
        )
        if result['results']:
            return result['results'][0]
        return None

    def get_users(
        self,
        keywords=None,
        user_ids=None,
        usernames=None,
        check_active=True,
        limit=SEARCH_RESULTS_LIMIT,
    ):
        """
        see the documentation for `cl uls` for information about keyword structure.
        """
        clauses = []
        offset = 0
        format_func = None
        count = False
        sort_key = [None]
        aux_fields = []  # Fields (e.g., sorting) that we need to include in the query

        # Number nested subqueries
        subquery_index = [0]

        def alias(clause):
            subquery_index[0] += 1
            return clause.alias('q' + str(subquery_index[0]))

        def is_numeric(key):
            return key in (
                'id',
                'time_quota',
                'parallel_run_quota',
                'time_used',
                'disk_quota',
                'disk_used',
            )

        def make_condition(key, field, value):
            # Special
            if value == '.sort':
                aux_fields.append(field)
                if is_numeric(key):
                    field = field * 1
                sort_key[0] = field
            elif value == '.sort-':
                aux_fields.append(field)
                if is_numeric(key):
                    field = field * 1
                sort_key[0] = desc(field)
            else:
                # Ordinary value
                if isinstance(value, list):
                    return field.in_(value)
                if '%' in value:
                    return field.like(value)
                return field == value
            return None

        if check_active:
            clauses.append(cl_user.c.is_active)
        if user_ids is not None:
            clauses.append(cl_user.c.user_id.in_(user_ids))
        if usernames is not None:
            clauses.append(or_(cl_user.c.user_name.in_(usernames), cl_user.c.email.in_(usernames)))
        if keywords is not None:
            for keyword in keywords:
                keyword = keyword.replace('.*', '%')
                # Sugar
                if keyword == '.count':
                    count = True
                    limit = None
                    continue
                elif keyword == '.last':
                    keyword = 'id=.sort-'

                m = SEARCH_KEYWORD_REGEX.match(keyword)  # key=value
                if m:
                    key, value = m.group(1), m.group(2)
                    if ',' in value:  # value is value1,value2
                        value = value.split(',')
                else:
                    key, value = '', keyword

                clause = None
                # Special functions
                if key == '.offset':
                    offset = int(value)
                elif key == '.limit':
                    limit = int(value)
                elif key == '.format':
                    format_func = value
                # Bundle fields
                elif key in (
                    'id',
                    'user_id',
                    'user_name',
                    'email',
                    'last_login',
                    'first_name',
                    'last_name',
                    'affiliation',
                    'time_quota',
                    'parallel_run_quota',
                    'time_used',
                    'disk_quota',
                    'disk_used',
                ):
                    clause = make_condition(key, getattr(cl_user.c, key), value)
                elif key == '.joined_after':
                    clause = cl_user.c.date_joined >= value
                elif key == '.active_after':
                    clause = cl_user.c.last_login >= value
                elif key == '.joined_before':
                    clause = cl_user.c.date_joined <= value
                elif key == '.active_before':
                    clause = cl_user.c.last_login <= value
                elif any(kw in key for kw in ['.disk', '.time']):
                    if '%' in value:
                        value = float(value.strip('%')) / 100.0
                    if key == '.disk_used_less_than':
                        clause = cl_user.c.disk_used / cl_user.c.disk_quota <= value
                    elif key == '.disk_used_more_than':
                        clause = cl_user.c.disk_used / cl_user.c.disk_quota >= value
                    elif key == '.time_used_less_than':
                        clause = cl_user.c.time_used / cl_user.c.time_quota <= value
                    elif key == '.time_used_more_than':
                        clause = cl_user.c.time_used / cl_user.c.time_quota >= value
                elif key == '':  # Match any field
                    clause = []
                    clause.append(cl_user.c.user_id.like('%' + value + '%'))
                    clause.append(cl_user.c.user_name.like('%' + value + '%'))
                    clause.append(cl_user.c.first_name.like('%' + value + '%'))
                    clause.append(cl_user.c.last_name.like('%' + value + '%'))
                    clause = or_(*clause)

                else:
                    raise UsageError('Unknown key: %s' % key)

                if clause is not None:
                    clauses.append(clause)

        clause = and_(*clauses)

        query = select([cl_user] + aux_fields).distinct().where(clause).offset(offset).limit(limit)

        # Sort
        if sort_key[0] is not None:
            query = query.order_by(sort_key[0])

        # Count
        if count:
            query = alias(query).count()

        with self.engine.begin() as connection:
            rows = connection.execute(query).fetchall()

        if count:  # Just returning a single number
            rows = worksheet_util.apply_func(format_func, rows[0][0])
            return {'results': rows, 'is_aggregate': True}
        else:
            results = [User(row) for row in rows]
            return {'results': results, 'is_aggregate': False}

    def user_exists(self, username, email):
        """
        Check whether user with given username or email exists.
        :param username: username
        :param email: email
        :return: True iff user with EITHER matching username or email exists.
        """
        with self.engine.begin() as connection:
            row = connection.execute(
                select([cl_user])
                .where(or_(cl_user.c.user_name == username, cl_user.c.email == email))
                .limit(1)
            ).fetchone()

        return row is not None and row.is_active

    def add_user(
        self,
        username,
        email,
        first_name,
        last_name,
        password,
        affiliation,
        notifications=NOTIFICATIONS_GENERAL,
        user_id=None,
        is_verified=False,
        has_access=False,
        time_used=0,
        disk_used=0,
        avatar_id=None,
    ):
        """
        Create a brand new unverified user.
        :param username:
        :param email:
        :param first_name:
        :param last_name:
        :param password:
        :param affiliation:
        :return: (new integer user ID, verification key to send)
        """
        with self.engine.begin() as connection:
            now = datetime.datetime.utcnow()
            user_id = user_id or '0x%s' % uuid4().hex

            connection.execute(
                cl_user.insert().values(
                    {
                        "user_id": user_id,
                        "user_name": username,
                        "email": email,
                        "notifications": notifications,
                        "last_login": None,
                        "is_active": True,
                        "first_name": first_name,
                        "last_name": last_name,
                        "date_joined": now,
                        "has_access": has_access,
                        "is_verified": is_verified,
                        "is_superuser": False,
                        "password": User.encode_password(password, crypt_util.get_random_string()),
                        "time_quota": self.default_user_info['time_quota'],
                        "parallel_run_quota": self.default_user_info['parallel_run_quota'],
                        "time_used": time_used,
                        "disk_quota": self.default_user_info['disk_quota'],
                        "disk_used": disk_used,
                        "affiliation": affiliation,
                        "url": None,
                        "avatar_id": avatar_id,
                    }
                )
            )

            if is_verified:
                verification_key = None
            else:
                verification_key = uuid4().hex
                connection.execute(
                    cl_user_verification.insert().values(
                        {
                            "user_id": user_id,
                            "date_created": now,
                            "date_sent": now,
                            "key": verification_key,
                        }
                    )
                )

        return user_id, verification_key

    def delete_user(self, user_id=None):
        """
        Delete the user with the given uuid.
        Delete all items in the database with a
        foreign key that references the user.

        :param user_id: id of user to delete
        """
        with self.engine.begin() as connection:

            # User verification
            connection.execute(
                cl_user_verification.delete().where(cl_user_verification.c.user_id == user_id)
            )
            connection.execute(
                cl_user_reset_code.delete().where(cl_user_reset_code.c.user_id == user_id)
            )

            # OAuth2
            connection.execute(
                oauth2_auth_code.delete().where(oauth2_auth_code.c.user_id == user_id)
            )
            connection.execute(oauth2_token.delete().where(oauth2_token.c.user_id == user_id))
            connection.execute(oauth2_client.delete().where(oauth2_client.c.user_id == user_id))

            # Workers
            connection.execute(cl_worker_run.delete().where(cl_worker_run.c.user_id == user_id))

            # User Groups
            connection.execute(cl_user_group.delete().where(cl_user_group.c.user_id == user_id))

            # Chat
            connection.execute(
                cl_chat.delete().where(
                    cl_chat.c.sender_user_id == user_id or cl_chat.c.recipient_user_id == user_id
                )
            )

            # Delete User
            connection.execute(cl_user.delete().where(cl_user.c.user_id == user_id))

    def get_verification_key(self, user_id):
        """
        Get verification key for given user.
        If one does not exist yet, create one and return it.
        Updates the "date_sent" field of the verification key to the current date.

        :param user_id: id of user to get verification key for
        :return: verification key, or None if none found for user
        """
        with self.engine.begin() as connection:
            verify_row = connection.execute(
                cl_user_verification.select()
                .where(cl_user_verification.c.user_id == user_id)
                .limit(1)
            ).fetchone()

            if verify_row is None:
                key = uuid4().hex
                now = datetime.datetime.utcnow()
                connection.execute(
                    cl_user_verification.insert().values(
                        {"user_id": user_id, "date_created": now, "date_sent": now, "key": key}
                    )
                )
            else:
                key = verify_row.key
                # Update date sent
                connection.execute(
                    cl_user_verification.update()
                    .where(cl_user_verification.c.user_id == user_id)
                    .values({"date_sent": datetime.datetime.utcnow()})
                )

        return key

    def verify_user(self, key):
        """
        Verify user with given verification key.
        :param key: verification key
        :return: True iff succeeded
        """
        with self.engine.begin() as connection:
            verify_row = connection.execute(
                cl_user_verification.select().where(cl_user_verification.c.key == key).limit(1)
            ).fetchone()

            # No matching key found
            if verify_row is None:
                return False

            # Delete matching verification key
            connection.execute(
                cl_user_verification.delete().where(cl_user_verification.c.key == key)
            )

            # Update user to be verified
            connection.execute(
                cl_user.update()
                .where(cl_user.c.user_id == verify_row.user_id)
                .values({"is_verified": True})
            )

        return True

    def is_verified(self, user_id):
        """
        Checks if the user is verified or not.
        :param user_id: id of the user
        :return: boolean to indicate if the user is verified or not
        """
        with self.engine.begin() as connection:
            verified_row = connection.execute(
                cl_user.select()
                .where(and_(cl_user.c.user_id == user_id, cl_user.c.is_verified))
                .limit(1)
            ).fetchone()

            return verified_row is not None

    def new_user_reset_code(self, user_id):
        """
        Generate a new password reset code.
        :param user_id: user_id of user for whom to reset password
        :return: reset code
        """
        with self.engine.begin() as connection:
            now = datetime.datetime.utcnow()
            code = uuid4().hex

            connection.execute(
                cl_user_reset_code.insert().values(
                    {"user_id": user_id, "date_created": now, "code": code}
                )
            )

        return code

    def get_reset_code_user_id(self, code, delete=False):
        """
        Check if reset code is valid.
        :param code: reset code
        :param delete: True iff delete code when found
        :return: user_id of associated user if succeeded, None otherwise
        """
        with self.engine.begin() as connection:
            reset_code_row = connection.execute(
                cl_user_reset_code.select().where(cl_user_reset_code.c.code == code).limit(1)
            ).fetchone()

            # No matching key found
            if reset_code_row is None:
                return None

            user_id = reset_code_row.user_id

            # Already done if not deleting code
            if not delete:
                return user_id

            # Delete matching reset code
            connection.execute(cl_user_reset_code.delete().where(cl_user_reset_code.c.code == code))

        return user_id

    def get_user_info(self, user_id, fetch_extra=False):
        """
        Return the user info corresponding to |user_id|.
        If a user doesn't exist, create a new one and set sane defaults.

        TODO(skoo): merge with get_user when wiring new user system together?
        """
        with self.engine.begin() as connection:
            rows = connection.execute(select([cl_user]).where(cl_user.c.user_id == user_id))
            user_info = None
            for row in rows:
                user_info = str_key_dict(row)
            if not user_info:
                raise NotFoundError("User with ID %s not found" % user_id)
            # Convert datetimes to strings to prevent JSON serialization errors
            if fetch_extra:
                if 'date_joined' in user_info and user_info['date_joined'] is not None:
                    user_info['date_joined'] = user_info['date_joined'].strftime('%Y-%m-%d')
                if 'last_login' in user_info and user_info['last_login'] is not None:
                    user_info['last_login'] = user_info['last_login'].strftime('%Y-%m-%d')
                user_info['is_root_user'] = user_info['user_id'] == self.root_user_id
                user_info['root_user_id'] = self.root_user_id
                user_info['system_user_id'] = self.system_user_id
            else:
                del user_info['date_joined']
                del user_info['last_login']
        return user_info

    def update_user_info(self, user_info):
        """
        Update the given user's info with |user_info|.
        """
        with self.engine.begin() as connection:
            connection.execute(
                cl_user.update().where(cl_user.c.user_id == user_info['user_id']).values(user_info)
            )

    def increment_user_time_used(self, user_id, amount):
        """
        User used some time.
        """
        user_info = self.get_user_info(user_id)
        user_info['time_used'] += amount
        self.update_user_info(user_info)

    def get_user_time_quota_left(self, user_id, user_info=None):
        if not user_info:
            user_info = self.get_user_info(user_id)
        time_quota = user_info['time_quota']
        time_used = user_info['time_used']
        return time_quota - time_used

    def get_user_parallel_run_quota_left(self, user_id, user_info=None):
        if not user_info:
            user_info = self.get_user_info(user_id)
        parallel_run_quota = user_info['parallel_run_quota']
        with self.engine.begin() as connection:
            # Get all the runs belonging to this user whose workers are not personal workers
            # of the user themselves
            active_runs = connection.execute(
                select([cl_worker_run.c.run_uuid]).where(
                    and_(
                        cl_worker_run.c.run_uuid.in_(
                            select([cl_bundle.c.uuid]).where(cl_bundle.c.owner_id == user_id)
                        ),
                        cl_worker_run.c.user_id != user_id,
                    )
                )
            ).fetchall()
        return parallel_run_quota - len(active_runs)

    def update_user_last_login(self, user_id):
        """
        Update user's last login date to now.
        """
        self.update_user_info({'user_id': user_id, 'last_login': datetime.datetime.utcnow()})

    def _get_disk_used(self, user_id):
        # TODO(Ashwin): don't include linked bundles
        return (
            self.search_bundles(user_id, ['size=.sum', 'owner_id=' + user_id, 'data_hash=%'])[
                'result'
            ]
            or 0
        )

    def get_user_disk_quota_left(self, user_id, user_info=None):
        if not user_info:
            user_info = self.get_user_info(user_id)
        return user_info['disk_quota'] - user_info['disk_used']

    def update_user_disk_used(self, user_id):
        user_info = self.get_user_info(user_id)
        # Compute from scratch for simplicity
        user_info['disk_used'] = self._get_disk_used(user_id)
        self.update_user_info(user_info)

    # ===========================================================================
    # OAuth-related methods follow!
    # ===========================================================================

    def _create_default_clients(self):
        DEFAULT_CLIENTS = [
            ('codalab_cli_client', 'CodaLab CLI'),
            ('codalab_worker_client', 'CodaLab Worker'),
        ]

        for client_id, client_name in DEFAULT_CLIENTS:
            if not self.get_oauth2_client(client_id):
                self.save_oauth2_client(
                    OAuth2Client(
                        self,
                        client_id=client_id,
                        secret=None,
                        name=client_name,
                        user_id=None,
                        grant_type='password',
                        response_type='token',
                        scopes='default',
                        redirect_uris='',
                    )
                )

    def get_oauth2_client(self, client_id):
        with self.engine.begin() as connection:
            row = connection.execute(
                select([oauth2_client]).where(oauth2_client.c.client_id == client_id).limit(1)
            ).fetchone()

        if row is None:
            return None

        return OAuth2Client(self, **row)

    def save_oauth2_client(self, client):
        with self.engine.begin() as connection:
            result = connection.execute(oauth2_client.insert().values(client.columns))
            client.id = result.lastrowid
        return client

    def get_oauth2_token(self, access_token=None, refresh_token=None):
        if access_token is not None:
            clause = oauth2_token.c.access_token == access_token
        elif refresh_token is not None:
            clause = oauth2_token.c.refresh_token == refresh_token
        else:
            return None

        with self.engine.begin() as connection:
            row = connection.execute(select([oauth2_token]).where(clause).limit(1)).fetchone()

        if row is None:
            return None

        return OAuth2Token(self, **row)

    def find_oauth2_token(self, client_id, user_id, expires_after):
        with self.engine.begin() as connection:
            row = connection.execute(
                select([oauth2_token])
                .where(
                    and_(
                        oauth2_token.c.client_id == client_id,
                        oauth2_token.c.user_id == user_id,
                        oauth2_token.c.expires > expires_after,
                    )
                )
                .limit(1)
            ).fetchone()

        if row is None:
            return None

        return OAuth2Token(self, **row)

    def save_oauth2_token(self, token):
        with self.engine.begin() as connection:
            result = connection.execute(oauth2_token.insert().values(token.columns))
            token.id = result.lastrowid
        return token

    def clear_oauth2_tokens(self, client_id, user_id):
        with self.engine.begin() as connection:
            connection.execute(
                oauth2_token.delete().where(
                    and_(
                        oauth2_token.c.client_id == client_id,
                        oauth2_token.c.user_id == user_id,
                        oauth2_token.c.expires <= datetime.datetime.utcnow(),
                    )
                )
            )

    def delete_oauth2_token(self, token_id):
        with self.engine.begin() as connection:
            connection.execute(oauth2_auth_code.delete().where(oauth2_token.c.id == token_id))

    def get_oauth2_auth_code(self, client_id, code):
        with self.engine.begin() as connection:
            row = connection.execute(
                select([oauth2_auth_code])
                .where(
                    and_(oauth2_auth_code.c.client_id == client_id, oauth2_auth_code.c.code == code)
                )
                .limit(1)
            ).fetchone()

        if row is None:
            return None

        return OAuth2AuthCode(self, **row)

    def save_oauth2_auth_code(self, grant):
        with self.engine.begin() as connection:
            result = connection.execute(oauth2_auth_code.insert().values(grant.columns))
            grant.id = result.lastrowid
        return grant

    def delete_oauth2_auth_code(self, auth_code_id):
        with self.engine.begin() as connection:
            connection.execute(
                oauth2_auth_code.delete().where(oauth2_auth_code.c.id == auth_code_id)
            )

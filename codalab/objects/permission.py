'''
Defines ORM classes for groups and permissions.
'''
from codalab.model.orm_object import ORMObject
from codalab.common import (
    NotFoundError,
    precondition,
    UsageError,
    PermissionError,
    IntegrityError,
)
from codalab.lib import (
    spec_util,
)
from codalab.model.tables import (
    GROUP_OBJECT_PERMISSION_ALL,
    GROUP_OBJECT_PERMISSION_READ,
    GROUP_OBJECT_PERMISSION_NONE,
    group_bundle_permission as cl_group_bundle_permission,
    group_object_permission as cl_group_worksheet_permission,
)
from codalab.model.util import LikeQuery


############################################################

def unique_group(model, group_spec, user_id):
    '''
    Return a group_info corresponding to |group_spec|.
    If |user_id| is given, only search only group that the user is involved in
    (either as an owner or just as a regular member).
    Otherwise, search all groups (this happens when we're root).
    '''
    def search_all(model, **spec_filters):
        return model.batch_get_groups(**spec_filters)
    def search_user(model, **spec_filters):
        return model.batch_get_all_groups(
            spec_filters,
            {'owner_id': user_id, 'user_defined': True},
            {'user_id': user_id})
    if user_id == None:
        search = search_all
    else:
        search = search_user
    return get_single_group(model, group_spec, search)

def get_single_group(model, group_spec, search_fn):
    '''
    Helper function.
    Resolve a string group_spec to a unique group for the given |search_fn|.
    Throw an error if zero or more than one group matches.
    '''
    if not group_spec:
        raise UsageError('Tried to expand empty group_spec!')
    if spec_util.UUID_REGEX.match(group_spec):
        groups = search_fn(model, uuid=group_spec)
        message = "uuid starting with '%s'" % (group_spec,)
    elif spec_util.UUID_PREFIX_REGEX.match(group_spec):
        groups = search_fn(model, uuid=LikeQuery(group_spec + '%'))
        message = "uuid starting with '%s'" % (group_spec,)
    else:
        spec_util.check_name(group_spec)
        groups = search_fn(model, name=group_spec)
        message = "name '%s'" % (group_spec,)
    if not groups:
        raise NotFoundError('Found no group with %s' % (message,))
    elif len(groups) > 1:
        raise UsageError(
          'Found multiple groups with %s:%s' %
          (message, ''.join('\n  uuid=%s' % (group['uuid'],) for group in groups))
        )
    return groups[0]

############################################################
# Checking permissions

def _check_permissions(model, table, user, object_uuids, owner_ids, need_permission):
    if len(object_uuids) == 0:
        return
    have_permissions = model.get_user_permissions(table, user.unique_id if user else None, object_uuids, owner_ids)
    #print '_check_permissions %s %s, have %s, need %s' % (user, object_uuids, map(permission_str, have_permissions.values()), permission_str(need_permission))
    if min(have_permissions.values()) >= need_permission:
        return
    if user:
        user_str = '%s(%s)' % (user.name, user.unique_id)
    else:
        user_str = None
    if table == cl_group_bundle_permission:
        object_type = 'bundle'
    elif table == cl_group_worksheet_permission:
        object_type = 'worksheet'
    else:
        raise IntegrityError('Unexpected table: %s' % table)
    raise PermissionError("User %s does not have sufficient permissions on %s %s (have %s, need %s)." % \
        (user_str, object_type, ' '.join(object_uuids), ' '.join(map(permission_str, have_permissions.values())), permission_str(need_permission)))

def check_bundles_have_read_permission(model, user, bundle_uuids):
    _check_permissions(model, cl_group_bundle_permission, user, bundle_uuids, model.get_bundle_owner_ids(bundle_uuids), GROUP_OBJECT_PERMISSION_READ)
def check_bundles_have_all_permission(model, user, bundle_uuids):
    _check_permissions(model, cl_group_bundle_permission, user, bundle_uuids, model.get_bundle_owner_ids(bundle_uuids), GROUP_OBJECT_PERMISSION_ALL)

def check_worksheet_has_read_permission(model, user, worksheet):
    _check_permissions(model, cl_group_worksheet_permission, user, [worksheet.uuid], {worksheet.uuid: worksheet.owner_id}, GROUP_OBJECT_PERMISSION_READ)
def check_worksheet_has_all_permission(model, user, worksheet):
    _check_permissions(model, cl_group_worksheet_permission, user, [worksheet.uuid], {worksheet.uuid: worksheet.owner_id}, GROUP_OBJECT_PERMISSION_ALL)

# We use a simpler permission model for running bundles. Only the root user
# or the user who owns the bundle can run it.
def check_bundle_have_run_permission(model, user_id, bundle):
    return user_id in [model.root_user_id, bundle.owner_id]

############################################################
# Parsing functions for permissions.

def parse_permission(permission_str):
    if 'r' == permission_str or 'read' == permission_str:
        return GROUP_OBJECT_PERMISSION_READ
    if 'a' == permission_str or 'all' == permission_str:
        return GROUP_OBJECT_PERMISSION_ALL
    if 'n' == permission_str or 'none' == permission_str:
        return GROUP_OBJECT_PERMISSION_NONE
    raise UsageError("Invalid permission flag specified (%s)" % (permission_str))

def permission_str(permission):
    if permission == 0: return 'none'
    if permission == 1: return 'read'
    if permission == 2: return 'all'
    raise UsageError("Invalid permission: %s" % permission)

# [{'group_name':'a', 'permission:1}, {'group_name':'b', 'permission':2}] => 'a:read,b:all'
def group_permissions_str(group_permissions):
    """
    Reads group ID from ['group']['id'] on each permission
    if |use_rest| is True, or ['group_uuid'] otherwise.
    """
    if len(group_permissions) == 0:
        return '-'
    return ','.join(
        '%s(%s):%s' % (row['group_name'],
                       row['group']['id'][0:8],
                       permission_str(row['permission']))
        for row in group_permissions)

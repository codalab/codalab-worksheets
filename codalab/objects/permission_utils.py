from codalab.common import UsageError

############################################################
# Parsing functions for permissions.


def parse_permission(permission_str):
    from codalab.model.tables import (
        GROUP_OBJECT_PERMISSION_ALL,
        GROUP_OBJECT_PERMISSION_READ,
        GROUP_OBJECT_PERMISSION_NONE,
    )

    if 'r' == permission_str or 'read' == permission_str:
        return GROUP_OBJECT_PERMISSION_READ
    if 'a' == permission_str or 'all' == permission_str:
        return GROUP_OBJECT_PERMISSION_ALL
    if 'n' == permission_str or 'none' == permission_str:
        return GROUP_OBJECT_PERMISSION_NONE
    raise UsageError("Invalid permission flag specified (%s)" % (permission_str))


def permission_str(permission):
    if permission == 0:
        return 'none'
    if permission == 1:
        return 'read'
    if permission == 2:
        return 'all'
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
        '%s(%s):%s'
        % (row['group_name'], row['group']['id'][0:8], permission_str(row['permission']))
        for row in group_permissions
    )

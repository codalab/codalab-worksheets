import os

from codalab.common import PermissionError, UsageError
from codalab.lib import (
    canonicalize,
    spec_util,
    worksheet_util,
)
from codalab.model.tables import GROUP_OBJECT_PERMISSION_READ
from codalab.objects.permission import (
    check_worksheet_has_all_permission,
)
from codalab.objects.user import PUBLIC_USER
from codalab.objects.worksheet import Worksheet
from codalab.rest.util import (
    local_bundle_client_compatible,
)


@local_bundle_client_compatible
def set_worksheet_permission(local, request, worksheet, group_uuid, permission):
    """
    Give the given |group_uuid| the desired |permission| on |worksheet_uuid|.
    """
    check_worksheet_has_all_permission(local.model, request.user, worksheet)
    local.model.set_group_worksheet_permission(group_uuid, worksheet.uuid, permission)


# FIXME(sckoo): fix when implementing worksheets API
@local_bundle_client_compatible
def populate_dashboard(local, request, worksheet):
    raise NotImplementedError("Automatic dashboard creation temporarily unsupported.")
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../objects/dashboard.ws')
    lines = [line.rstrip() for line in open(file_path, 'r').readlines()]
    items, commands = worksheet_util.parse_worksheet_form(lines, self, worksheet.uuid)
    info = self.get_worksheet_info(worksheet.uuid, True)
    self.update_worksheet_items(info, items)
    self.update_worksheet_metadata(worksheet.uuid, {'title': 'Codalab Dashboard'})


@local_bundle_client_compatible
def get_worksheet_uuid_or_none(local, request, base_worksheet_uuid, worksheet_spec):
    """
    Helper: Return the uuid of the specified worksheet if it exists. Otherwise, return None.
    """
    try:
        return canonicalize.get_worksheet_uuid(local.model, base_worksheet_uuid, worksheet_spec)
    except UsageError:
        return None


@local_bundle_client_compatible
def ensure_unused_worksheet_name(local, request, name):
    """
    Ensure worksheet names are unique.
    Note: for simplicity, we are ensuring uniqueness across the system, even on
    worksheet names that the user may not have access to.
    """
    # If trying to set the name to a home worksheet, then it better be
    # user's home worksheet.
    if spec_util.is_home_worksheet(name) and spec_util.home_worksheet(request.user.user_name) != name:
        raise UsageError('Cannot create %s because this is potentially the home worksheet of another user' % name)
    if get_worksheet_uuid_or_none(None, name) is not None:
        raise UsageError('Worksheet with name %s already exists' % name)


@local_bundle_client_compatible
def new_worksheet(local, request, name):
    """
    Create a new worksheet with the given |name|.
    """
    if request.user is PUBLIC_USER:
        raise PermissionError("You must be logged in to create a worksheet.")
    ensure_unused_worksheet_name(name)

    # Don't need any permissions to do this.
    worksheet = Worksheet({
        'name': name,
        'title': None,
        'frozen': None,
        'items': [],
        'owner_id': request.user.user_id
    })
    local.model.new_worksheet(worksheet)

    # Make worksheet publicly readable by default
    set_worksheet_permission(worksheet, local.model.public_group_uuid,
                             GROUP_OBJECT_PERMISSION_READ)
    if spec_util.is_dashboard(name):
        populate_dashboard(worksheet)
    return worksheet.uuid


@local_bundle_client_compatible
def get_worksheet_uuid(local, request, base_worksheet_uuid, worksheet_spec):
    """
    Return the uuid of the specified worksheet if it exists.
    If not, create a new worksheet if the specified worksheet is home_worksheet
    or dashboard. Otherwise, throw an error.
    """
    if worksheet_spec == '' or worksheet_spec == worksheet_util.HOME_WORKSHEET:
        worksheet_spec = spec_util.home_worksheet(request.user.user_name)
    worksheet_uuid = get_worksheet_uuid_or_none(base_worksheet_uuid, worksheet_spec)
    if worksheet_uuid is not None:
        return worksheet_uuid
    else:
        if spec_util.is_home_worksheet(worksheet_spec) or spec_util.is_dashboard(worksheet_spec):
            return new_worksheet(worksheet_spec)
        else:
            # let it throw the correct error message
            return canonicalize.get_worksheet_uuid(local.model, base_worksheet_uuid, worksheet_spec)

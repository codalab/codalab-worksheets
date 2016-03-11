import httplib

from bottle import abort, local, request

from codalab.common import PermissionError, UsageError
from codalab.objects.permission import check_worksheet_has_all_permission, check_worksheet_has_read_permission


def safe_get_worksheet(uuid, need_read=False, need_all=False):
    """
    Reads the worksheet from the database, checking for any required permissions.
    Adapts any errors to HTTP errors.
    """
    try:
        worksheet = local.model.get_worksheet(uuid, fetch_items=False)
    except UsageError as e:
        abort(httplib.NOT_FOUND, e.message)
    if need_read:
        try:
            check_worksheet_has_read_permission(local.model, request.user, worksheet)
        except PermissionError as e:
            abort(httplib.FORBIDDEN, e.message)
    if need_all:
        try:
            check_worksheet_has_all_permission(local.model, request.user, worksheet)
        except PermissionError as e:
            abort(httplib.FORBIDDEN, e.message)
    return worksheet

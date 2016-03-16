"""
These APIs are used to fill in the title for the worksheet and bundle detail
pages, since the Django app doesn't have access to that information when
rendering the templates. We want to have the title filled in as quickly as
possibly to maximize it being correct in a search engine crawl. Thus, we do it
in Javascript that is loaded in the <head> section.
"""

from bottle import get, local, request, template

from codalab.lib import spec_util
from codalab.objects.permission import check_bundles_have_read_permission, check_worksheet_has_read_permission


@get('/titlejs/worksheet/<uuid:re:%s>/' % spec_util.UUID_STR)
def get_worksheet_title_js(uuid):
    """
    Returns Javascript code that updates the worksheet title.
    """
    worksheet = local.model.get_worksheet(uuid, fetch_items=False)
    check_worksheet_has_read_permission(local.model, request.user, worksheet)
    if worksheet.title:
        title = worksheet.title
    else:
        title = worksheet.name
    return template('title_setter_js', title=title)


@get('/titlejs/bundle/<uuid:re:%s>/' % spec_util.UUID_STR)
def get_bundle_title_js(uuid):
    """
    Returns Javascript code that updates the bundle title.
    """
    check_bundles_have_read_permission(local.model, request.user, [uuid])
    bundle = local.model.get_bundle(uuid)
    return template('title_setter_js', title=bundle.metadata.name)

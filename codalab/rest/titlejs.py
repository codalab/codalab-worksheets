"""
These APIs are used to fill in the title for the worksheet and bundle detail
pages, since the Django app doesn't have access to that information when
rendering the templates. We want to have the title filled in as quickly as
possibly to maximize it being correct in a search engine crawl. Thus, we do it
in Javascript that is loaded in the <head> section.
"""

from bottle import get, template

from codalab.lib import spec_util
from codalab.rest.bundle import safe_get_bundle
from codalab.rest.worksheet import safe_get_worksheet


@get('/titlejs/worksheet/<uuid:re:%s>/' % spec_util.UUID_STR)
def get_worksheet_title_js(uuid):
    """
    Returns Javascript code that updates the worksheet title.
    """
    worksheet = safe_get_worksheet(uuid, need_read=True)
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
    bundle = safe_get_bundle(uuid, need_read=True)
    return template('title_setter_js', title=bundle.metadata.name)

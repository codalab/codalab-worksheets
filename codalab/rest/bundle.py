import httplib
import mimetypes

from bottle import abort, get, local, put, request, response

from codalab.lib import spec_util, zip_util
from codalab.objects.permission import (
    check_bundles_have_all_permission,
    check_bundles_have_read_permission,
)
from codalab.server.authenticated_plugin import AuthenticatedPlugin


@get('/bundles/<uuid:re:%s>/contents/blob/' % spec_util.UUID_STR)
@get('/bundles/<uuid:re:%s>/contents/blob/<path:path>' % spec_util.UUID_STR)
def get_blob(uuid, path=''):
    """
    API to download the contents of a bundle or a subpath within a bundle.

    For directories this method always returns a tarred and gzipped archive of
    the directory.

    For files, if the request has an Accept-Encoding header containing gzip,
    then the returned file is gzipped.
    """
    check_bundles_have_read_permission(local.model, request.user, [uuid])
    bundle = local.model.get_bundle(uuid)

    target_info = local.download_manager.get_target_info(uuid, path, 0)
    if target_info is None:
        abort(httplib.NOT_FOUND, 'Not found.')

    # Figure out the file name.
    if not path and bundle.metadata.name:
        filename = bundle.metadata.name
    else:
        filename = target_info['name']

    if target_info['type'] == 'directory':
        # Always tar and gzip directories.
        filename = filename + '.tar.gz'
        fileobj = local.download_manager.stream_tarred_gzipped_directory(uuid, path)
    elif target_info['type'] == 'file':
        if not zip_util.path_is_archive(filename) and request_accepts_gzip_encoding():
            # Let's gzip to save bandwidth. The browser will transparently decode
            # the file.
            filename = filename + '.gz'
            fileobj = local.download_manager.stream_file(uuid, path, gzipped=True)
        else:
            fileobj = local.download_manager.stream_file(uuid, path, gzipped=False)
    else:
        # Symlinks.
        abort(httplib.FORBIDDEN, 'Cannot download files of this type.')
    
    # Set headers.
    mimetype, _ = mimetypes.guess_type(filename, strict=False)
    response.set_header('Content-Type', mimetype or 'text/plain')
    if zip_util.get_archive_ext(filename) == '.gz' and request_accepts_gzip_encoding():
        filename = zip_util.strip_archive_ext(filename)
        response.set_header('Content-Encoding', 'gzip')
    else:
        response.set_header('Content-Encoding', 'identity')
    response.set_header('Content-Disposition', 'filename="%s"' % filename)

    return fileobj


@put('/bundles/<uuid:re:%s>/contents/blob/' % spec_util.UUID_STR,
     apply=AuthenticatedPlugin())
def update_blob(uuid):
    """
    Update the contents of the given running or uploading bundle.

    Accepts the filename as a query parameter, used to determine whether the
    upload contains an archive.
    """
    check_bundles_have_all_permission(local.model, request.user, [uuid])
    bundle = local.model.get_bundle(uuid)

    # If this bundle already has data, remove it.
    if local.upload_manager.has_contents(bundle):
        local.upload_manager.cleanup_existing_contents(bundle)

    # Store the data.
    try:
        local.upload_manager.upload_to_bundle_store(
            bundle, sources=[(request.query.filename, request['wsgi.input'])],
            follow_symlinks=False, exclude_patterns=False, remove_sources=False,
            git=False, unpack=True, simplify_archives=False)
        local.upload_manager.update_metadata_and_save(bundle, new_bundle=False)

    except Exception:
        if local.upload_manager.has_contents(bundle):
            local.upload_manager.cleanup_existing_contents(bundle)
        raise


def request_accepts_gzip_encoding():
    # See rules for parsing here: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
    # Browsers silently decode gzipped files, so we save some bandwidth.
    if 'Accept-Encoding' not in request.headers:
        return False
    for encoding in request.headers['Accept-Encoding'].split(','):
        encoding = encoding.strip().split(';')
        if encoding[0] == 'gzip':
            if len(encoding) > 1 and encoding[1] == 'q=0':
                return False
            else:
                return True
    return False

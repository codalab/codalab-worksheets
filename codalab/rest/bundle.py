import httplib
import mimetypes
import os.path
import subprocess

from bottle import abort, get, local, request, response

from codalab.lib import path_util, spec_util
from codalab.objects.permission import check_bundles_have_read_permission

@get('/bundle/<uuid:re:%s>/contents/blob/' % spec_util.UUID_STR)
@get('/bundle/<uuid:re:%s>/contents/blob/<path:path>' % spec_util.UUID_STR)
def get_blob(uuid, path=''):
    """
    API to download the contents of a bundle or a subpath within a bundle.

    Directories are always archived. Files are archived only if the archive
    query parameter is set to 1.
    """
    archive = 'archive' in request.query and request.query['archive'] == '1'

    check_bundles_have_read_permission(local.model, request.user, [uuid])
    bundle = local.model.get_bundle(uuid)

    # Find the data.
    bundle_root = os.path.realpath(local.bundle_store.get_bundle_location(uuid))
    final_path = os.path.realpath(path_util.safe_join(bundle_root, path))

    # Check for errors.
    if not final_path.startswith(bundle_root):
        abort(httplib.BAD_REQUEST,
              'Invalid target %s in bundle %s' % (path, uuid))
    if not os.path.exists(final_path):
        abort(httplib.NOT_FOUND,
              'Invalid target %s in bundle %s' % (path, uuid))

    # Figure out the file name.
    if path:
        filename = os.path.basename(path)
    elif bundle.metadata.name:
        filename = bundle.metadata.name
    else:
        filename = uuid

    # Archive, if needed.
    if os.path.isdir(final_path):
        filename = filename + '.tar.gz'
        args = ['tar', 'czf', '-', '-C', final_path]
        files = os.listdir(final_path)
        if files:
            args.extend(files)
        else:
            args.extend(['--files-from', '/dev/null'])
        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
        fileobj = proc.stdout
    elif archive:
        filename = filename + '.gz'
        args = ['gzip', '-c', '-n', final_path]
        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
        fileobj = proc.stdout
    else:
        fileobj = open(final_path, 'rb')

    # Set headers.
    mimetype, encoding = mimetypes.guess_type(filename, strict=False)
    response.set_header('Content-Disposition', 'filename="%s"' % filename)
    response.set_header('Content-Type', mimetype or 'text/plain')
    response.set_header('Content-Encoding', encoding or 'identity')

    return fileobj

import httplib
import mimetypes
import os.path
import re
import shutil
import subprocess
import tarfile
import zlib

from bottle import abort, get, local, put, request, response

from codalab.common import PermissionError, UsageError
from codalab.lib import path_util, spec_util
from codalab.objects.permission import check_bundles_have_all_permission, check_bundles_have_read_permission
from codalab.server.authenticated_plugin import AuthenticatedPlugin


def safe_get_bundle(uuid):
    try:
        return local.model.get_bundle(uuid)
    except UsageError as e:
        abort(httplib.NOT_FOUND, e.message)


@get('/bundle/<uuid:re:%s>/contents/blob/' % spec_util.UUID_STR)
@get('/bundle/<uuid:re:%s>/contents/blob/<path:path>' % spec_util.UUID_STR)
def get_blob(uuid, path=''):
    """
    API to download the contents of a bundle or a subpath within a bundle.

    Directories are always archived. Files are archived only if the archive
    query parameter is set to 1.
    """
    archive = 'archive' in request.query and request.query['archive'] == '1'

    try:
        check_bundles_have_read_permission(local.model, request.user, [uuid])
    except PermissionError as e:
        abort(httplib.FORBIDDEN, e.message)

    bundle = safe_get_bundle(uuid)

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


@put('/bundle/<uuid:re:%s>/contents/blob/' % spec_util.UUID_STR,
     apply=AuthenticatedPlugin())
def put_blob(uuid):
    try:
        check_bundles_have_all_permission(local.model, request.user, [uuid])
    except PermissionError as e:
        abort(httplib.FORBIDDEN, e.message)

    bundle = safe_get_bundle(uuid)

    # If this bundle already has data, remove it.
    bundle_path = os.path.realpath(local.bundle_store.get_bundle_location(uuid))
    if os.path.exists(bundle_path):
        local.bundle_store.cleanup(uuid, dry_run=False)
        bundle_update = {
            'data_hash': None,
        }
        local.model.update_bundle(bundle, bundle_update)

    # Figure out what kind of upload it is.
    tar_archive = False
    single_file_archive = False
    if 'Content-Disposition' in request.headers:
        match = re.search('filename="([^"]*)"',
                          request.headers['Content-Disposition'])
        if match:
            filename = match.group(1)
            if filename.endswith('.tar.gz'):
                tar_archive = True
            elif filename.endswith('.gz'):
                single_file_archive = True

    def do_abort(code, message):
        local.bundle_store.cleanup(uuid, dry_run=False)
        abort(code, message)

    # Store the data.
    input_stream = request['wsgi.input']
    if tar_archive:
        os.mkdir(bundle_path)
        try:
            with tarfile.open(fileobj=input_stream, mode='r|gz') as tar:
                for member in tar:
                    # Make sure that there is no trickery going on (see note in
                    # TarFile.extractall() documentation.
                    member_path = os.path.realpath(os.path.join(bundle_path, member.name))
                    if not member_path.startswith(bundle_path):
                        do_abort(httplib.BAD_REQUEST, 'Invalid archive')

                    tar.extract(member, bundle_path)
        except tarfile.TarError as e:
            do_abort(httplib.BAD_REQUEST, 'Invalid archive')
    elif single_file_archive:
        try:
            d = zlib.decompressobj(16 + zlib.MAX_WBITS)
            with open(bundle_path, 'wb') as f:
                while True:
                    chunk = input_stream.read(16 * 1024)
                    if not chunk:
                        break
                    f.write(d.decompress(chunk))
                f.write(d.flush())
        except zlib.error as e:
            do_abort(httplib.BAD_REQUEST, 'Invalid archive')
    else:
        with open(bundle_path, 'wb') as f:
            shutil.copyfileobj(input_stream, f)

    bundle_update = {
       'data_hash': '0x%s' % path_util.hash_path(bundle_path),
       'metadata': {
            'data_size': path_util.get_size(bundle_path),             
        },
    }
    local.model.update_bundle(bundle, bundle_update)
    local.model.update_user_disk_used(bundle.owner_id)

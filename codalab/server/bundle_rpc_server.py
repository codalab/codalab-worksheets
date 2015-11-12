'''
BundleRPCServer is a FileServer that opens a BundleClient and then exposes that
client's API methods as RPC methods for a RemoteBundleClient.

Methods that take JSON-able input and return JSON-able output (that is, methods
in RemoteBundleClient.CLIENT_COMMANDS) are simply passed to the internal client.

Other methods, like upload and cat, are more complicated because they perform
filesystem operations. BundleRPCServer supports variants of these methods:
  upload_bundle_zip: used to implement RemoteBundleClient.upload
  open_target: used to implement RemoteBundleClient.cat

Important: each call to open_temp_file, open_target, open_target_zip should
have a matching call to finalize_file.
'''
import tempfile
import traceback
import os
import time

from codalab.common import (
    precondition,
    UsageError,
    PermissionError,
)
from codalab.client.remote_bundle_client import RemoteBundleClient
from codalab.lib import zip_util, path_util
from codalab.server.file_server import FileServer


class BundleRPCServer(FileServer):
    def __init__(self, manager):
        self.host = manager.config['server']['host']
        self.port = manager.config['server']['port']
        self.verbose = manager.config['server']['verbose']
        # This server is backed by a LocalBundleClient that processes client commands
        self.client = manager.client('local', is_cli=False)

        # args might be a large object; summarize it (e.g., take prefixes of lists)
        def compress_args(args):
            if isinstance(args, basestring):
                if len(args) > 100:
                    args = args[:100] + '...'
            if isinstance(args, list):
                if len(args) > 4:
                    args = args[:4] + ['...']
            if isinstance(args, tuple):
                return tuple(map(compress_args, args))
            elif isinstance(args, list):
                return map(compress_args, args)
            elif isinstance(args, dict):
                return dict((compress_args(k), compress_args(v)) for k, v in args.items())
            return args

        tempdir = tempfile.gettempdir()  # Consider using CodaLab's temp directory
        FileServer.__init__(self, (self.host, self.port), tempdir, manager.auth_handler())

        def wrap(target, command):
            def function_to_register(*args, **kwargs):
                # Process args for logging
                if command == 'login':
                    log_args = args[:2]  # Don't log password
                else:
                    log_args = compress_args(args)

                if self.verbose >= 1:
                    print "bundle_rpc_server: %s %s" % (command, log_args)

                try:
                    start_time = time.time()

                    # Dynamically bind method and call it
                    result = getattr(target, command)(*args, **kwargs)

                    # Log this activity.
                    self.client.model.update_events_log(
                        start_time=start_time,
                        user_id=self.client._current_user_id(),
                        user_name=self.client._current_user_name(),
                        command=command,
                        args=log_args)

                    return result
                except Exception, e:
                    if not (isinstance(e, UsageError) or isinstance(e, PermissionError)):
                        # This is really bad and shouldn't happen.
                        # If it does, someone should get paged.
                        print '=== INTERNAL ERROR:', e
                        traceback.print_exc()
                    raise e

            return function_to_register

        for command in RemoteBundleClient.CLIENT_COMMANDS:
            self.register_function(wrap(self.client, command), command)

        for command in RemoteBundleClient.SERVER_COMMANDS:
            self.register_function(wrap(self, command), command)

    def upload_bundle_zip(self, file_uuid, construct_args, worksheet_uuid, follow_symlinks, add_to_worksheet):
        '''
        |file_uuid| specifies a pointer to the temporary file X.
        - If X is a non-zip file, then just upload X as an ordinary file.
        - If X is a zip file containing one file/directory Y representing bundle, then upload Y.
        - If X is a zip file containing multiple files/directories, then upload X.
        Return the new bundle's uuid.
        Note: delete the file_uuid file and X if needed (these are temporary files).
        '''
        if file_uuid:
            orig_path = self.file_paths[file_uuid]  # Note: cheat and look at file_server's data
            precondition(orig_path, 'Unexpected file uuid: %s' % (file_uuid,))
            if zip_util.is_zip_file(orig_path):
                container_path = tempfile.mkdtemp()  # Make temporary directory
                zip_util.unzip(orig_path, container_path, file_name=None)  # Unzip into a directory
                # If the container path only has one item, then make that the final path
                sub_files = os.listdir(container_path)
                if len(sub_files) == 1:
                    final_path = os.path.join(container_path, sub_files[0])
                else:  # Otherwise, use the container path
                    final_path = container_path
                    container_path = None
            else:
                # Not a zip file!  Just upload it normally as a file.
                final_path = orig_path
                container_path = None  # Don't need to delete
        else:
            final_path = None
        result = self.client.upload_bundle(final_path, construct_args, worksheet_uuid, follow_symlinks, exclude_patterns=[], add_to_worksheet=add_to_worksheet)
        if file_uuid:
            if container_path:
                path_util.remove(container_path)  # Remove temporary directory
            self.finalize_file(file_uuid, final_path != orig_path)  # Remove temporary file
        return result

    def open_target(self, target):
        '''
        Open a read-only file handle to the given bundle target and return a file
        uuid identifying it.
        '''
        path = self.client.get_target_path(target)
        return self.open_file(path)

    def open_target_zip(self, target, follow_symlinks):
        '''
        Return a file uuid for the zip file and the name that the zip file contains.
        '''
        bundle_uuid = target[0]
        path = self.client.get_target_path(target)
        name = self.client.get_bundle_info(bundle_uuid)['metadata']['name']
        zip_path = zip_util.zip(path, follow_symlinks=follow_symlinks, exclude_patterns=[], file_name=name)  # Create temporary zip file
        return self.open_file(zip_path), name

    def serve_forever(self):
        print 'BundleRPCServer serving to %s at port %s...' % ('ALL hosts' if self.host == '' else 'host ' + self.host, self.port)
        FileServer.serve_forever(self)

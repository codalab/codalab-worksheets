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

from codalab.client.remote_bundle_client import RemoteBundleClient
from codalab.common import precondition
from codalab.lib import zip_util, path_util
from codalab.server.file_server import FileServer


class BundleRPCServer(FileServer):
    def __init__(self, manager):
        self.host = manager.config['server']['host']
        self.port = manager.config['server']['port']
        self.verbose = manager.config['server']['verbose']
        # This server is backed by a LocalBundleClient that processes client commands
        self.client = manager.client('local', is_cli=False)

        tempdir = tempfile.gettempdir()  # Consider using CodaLab's temp directory
        FileServer.__init__(self, (self.host, self.port), tempdir, manager.auth_handler())
        def wrap(command, func):
            def inner(*args, **kwargs):
                if self.verbose >= 1:
                    print "bundle_rpc_server: %s %s" % (command, args)
                return func(*args, **kwargs)
            return inner
        for command in RemoteBundleClient.CLIENT_COMMANDS:
            self.register_function(wrap(command, getattr(self.client, command)), command)
        for command in RemoteBundleClient.SERVER_COMMANDS:
            self.register_function(wrap(command, getattr(self, command)), command)

    def upload_bundle_zip(self, file_uuid, construct_args, worksheet_uuid, follow_symlinks):
        '''
        Unzip the zip in the temp file identified by the given file uuid and then
        upload the unzipped directory. Return the new bundle's id.
        Note: delete the file_uuid file, because it's temporary!
        '''
        zip_path = self.file_paths[file_uuid]  # Note: cheat and look at file_server's data
        precondition(zip_path, 'Unexpected file uuid: %s' % (file_uuid,))
        container_path = tempfile.mkdtemp()  # Make temporary directory
        path = zip_util.unzip(zip_path, container_path)  # Unzip
        result = self.client.upload_bundle(path, construct_args, worksheet_uuid, follow_symlinks)
        path_util.remove(container_path)  # Remove temporary directory
        self.finalize_file(file_uuid, True)  # Remove temporary zip
        return result

    def open_target(self, target):
        '''
        Open a read-only file handle to the given bundle target and return a file
        uuid identifying it.
        '''
        path = self.client.get_target_path(target)
        return self.open_file(path, 'rb')

    def open_target_zip(self, target, follow_symlinks):
        path = self.client.get_target_path(target)
        name = self.client.get_bundle_info(target)['metadata']['name']
        zip_path, sub_path = zip_util.zip(path, follow_symlinks=follow_symlinks, file_name=name)  # Create temporary zip file
        return self.open_file(zip_path, 'rb'), sub_path

    def serve_forever(self):
        print 'BundleRPCServer serving to %s at port %s...' % ('ALL hosts' if self.host == '' else 'host ' + self.host, self.port)
        FileServer.serve_forever(self)

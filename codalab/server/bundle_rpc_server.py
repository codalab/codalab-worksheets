from codalab.client.local_bundle_client import LocalBundleClient
from codalab.client.remote_bundle_client import RemoteBundleClient
from codalab.common import (
  BUNDLE_RPC_PORT,
  precondition,
)
from codalab.lib import zip_util
from codalab.server.file_server import FileServer


class BundleRPCServer(FileServer):
  SERVER_COMMANDS = (
    'upload_zip',
    'open_target',
  )

  def __init__(self):
    address = ('localhost', BUNDLE_RPC_PORT)
    self.client = LocalBundleClient()
    FileServer.__init__(self, address, self.client.bundle_store.temp)
    for command in RemoteBundleClient.CLIENT_COMMANDS:
      self.register_function(getattr(self.client, command), command)
    for command in self.SERVER_COMMANDS:
      self.register_function(getattr(self, command), command)

  def upload_zip(self, bundle_type, file_uuid, metadata):
    '''
    Upload the zip in the temp file identified by the given file UUID.
    '''
    zip_path = self.temp_file_paths.pop(file_uuid, None)
    zip_directory = zip_util.unzip_directory(zip_path)
    precondition(zip_path, 'Unexpected file uuid: %s' % (file_uuid,))
    return self.client.upload(bundle_type, zip_directory, metadata)

  def open_target(self, target):
    '''
    Open a read-only file handle to the given bundle target.
    '''
    path = self.client.get_target_path(target)
    return self.open_file(path, 'rb')

  def serve_forever(self):
    print 'RPC server serving on port %s...' % (BUNDLE_RPC_PORT,)
    FileServer.serve_forever(self)

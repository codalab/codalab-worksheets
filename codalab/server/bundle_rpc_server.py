'''
BundleRPCServer is a FileServer that opens a BundleClient and then exposes that
client's API methods as RPC methods for a RemoteBundleClient.

Methods that take JSON-able input and return JSON-able output (that is, methods
in RemoteBundleClient.CLIENT_COMMANDS) are simply passed to the internal client.

Other methods, like upload and cat, are more complicated because they perform
filesystem operations. BundleRPCServer supports variants of these methods:
  upload_zip: used to implement RemoteBundleClient.upload
  open_target: used to implement RemoteBundleClient.cat
'''
import tempfile

from codalab.client.remote_bundle_client import RemoteBundleClient
from codalab.common import precondition
from codalab.lib import zip_util
from codalab.server.file_server import FileServer


class BundleRPCServer(FileServer):
  SERVER_COMMANDS = (
    'upload_zip',
    'open_target',
  )

  def __init__(self, address, client):
    self.client = client
    FileServer.__init__(self, address, tempfile.gettempdir())
    for command in RemoteBundleClient.CLIENT_COMMANDS:
      self.register_function(getattr(self.client, command), command)
    for command in self.SERVER_COMMANDS:
      self.register_function(getattr(self, command), command)

  def upload_zip(self, bundle_type, file_uuid, metadata):
    '''
    Unzip the zip in the temp file identified by the given file uuid and then
    upload the unzipped directory. Return the new bundle's id.
    '''
    zip_path = self.temp_file_paths.pop(file_uuid, None)
    zip_directory = zip_util.unzip_directory(zip_path)
    precondition(zip_path, 'Unexpected file uuid: %s' % (file_uuid,))
    return self.client.upload(bundle_type, zip_directory, metadata)

  def open_target(self, target):
    '''
    Open a read-only file handle to the given bundle target and return a file
    uuid identifying it.
    '''
    path = self.client.get_target_path(target)
    return self.open_file(path, 'rb')

  def serve_forever(self):
    FileServer.serve_forever(self)

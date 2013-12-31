import os
from SimpleXMLRPCServer import SimpleXMLRPCServer
import tempfile
import uuid
import xmlrpclib

from codalab.common import precondition
from codalab.lib import path_util


class FileServer(SimpleXMLRPCServer):
  FILE_SUBDIRECTORY = 'file'

  def __init__(self, address, temp):
    # Keep a a dictionary mapping file uuids to open file handles in a temp dir.
    self.file_uuid_map = {}
    self.temp = temp
    # Register file-like RPC methods to allow for file transfer.
    SimpleXMLRPCServer.__init__(self, address, allow_none=True)
    for fn_name in ('open_file', 'read_file', 'write_file', 'close_file'):
      self.register_function(getattr(self, fn_name), fn_name)

  def open_file(self, path=None):
    '''
    Open a read-only file handle to the path and return a UUID identifying it.
    If path is None, open a writeable file handle to a new temporary file.
    '''
    file_uuid = uuid.uuid4().hex
    (mode, read_only) = ('rb', True)
    if path is None:
      (mode, read_only) = ('wb', False)
      (fd, path) = tempfile.mkstemp(dir=self.temp)
      os.close(fd)
    else:
      path_util.check_isfile(path, 'open_file')
    file_handle = open(path, mode)
    self.file_uuid_map[file_uuid] = (file_handle, read_only)
    return file_uuid

  def read_file(self, file_uuid, num_bytes=None):
    '''
    Read up to num_bytes from the given file uuid. Return an empty buffer
    if and only if this file handle is at EOF.
    '''
    (file_handle, _) = self.file_uuid_map[file_uuid]
    return xmlrpclib.Binary(file_handle.read(num_bytes))

  def write_file(self, file_uuid, buffer):
    '''
    Write data from the given binary data buffer to the file uuid.
    '''
    (file_handle, read_only) = self.file_uuid_map[file_uuid]
    precondition(not read_only, 'Wrote to read-only file: %s' % (file_uuid,))
    file_handle.write(buffer.data)

  def close_file(self, file_uuid):
    '''
    Close the given file uuid.
    '''
    file_pair = self.file_uuid_map.pop(file_uuid, None)
    if file_pair:
      (file_handle, _) = file_pair
      file_handle.close()

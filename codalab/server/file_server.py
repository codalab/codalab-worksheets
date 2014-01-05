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
    # Keep a a dictionary mapping file uuids to open file handles and a
    # dictionary mapping temporary file's file uuids to their absolute paths.
    self.file_handles = {}
    self.temp_file_paths = {}
    self.temp = temp
    # Register file-like RPC methods to allow for file transfer.
    SimpleXMLRPCServer.__init__(self, address, allow_none=True)
    for fn_name in ('open_temp_file', 'read_file', 'write_file', 'close_file'):
      self.register_function(getattr(self, fn_name), fn_name)

  def open_file(self, path, mode):
    '''
    Open a file handle to the given path and return a UUID identifying it.
    '''
    path_util.check_isfile(path, 'open_file')
    file_uuid = uuid.uuid4().hex
    self.file_handles[file_uuid] = open(path, mode)
    return file_uuid

  def open_temp_file(self):
    '''
    Open a new temp file for write and return a file UUID identifying it.
    '''
    (fd, path) = tempfile.mkstemp(dir=self.temp)
    os.close(fd)
    file_uuid = self.open_file(path, 'wb')
    self.temp_file_paths[file_uuid] = path
    return file_uuid

  def read_file(self, file_uuid, num_bytes=None):
    '''
    Read up to num_bytes from the given file uuid. Return an empty buffer
    if and only if this file handle is at EOF.
    '''
    file_handle = self.file_handles[file_uuid]
    return xmlrpclib.Binary(file_handle.read(num_bytes))

  def write_file(self, file_uuid, buffer):
    '''
    Write data from the given binary data buffer to the file uuid.
    '''
    file_handle = self.file_handles[file_uuid]
    file_handle.write(buffer.data)

  def close_file(self, file_uuid):
    '''
    Close the given file uuid.
    '''
    file_handle = self.file_handles.pop(file_uuid, None)
    if file_handle:
      file_handle.close()

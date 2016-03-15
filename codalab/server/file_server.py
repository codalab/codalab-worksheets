'''
FileServer is a library with a file-like interface for reading and writing,
most of which is exposed by the BundleRPCServer for calling remotely.

The core method that opens files handles, open_file, is NOT exposed as an RPC
method for security reasons. Instead, alternate methods for opening files (such
as open_temp_file) are exposed by this class. These methods all return a file
uuid, which is like a Unix file descriptor.

The other methods, such as read_file, write_file, and close_file, are exposed
as RPC methods. These methods take a file uuid in addition to their regular
arguments, and they perform the requested operation on the file handle
corresponding to that uuid.
'''
import os
import tempfile
import uuid
import xmlrpclib

from codalab.lib import path_util, zip_util


class FileServer(object):
    def __init__(self):
        # Keep a dictionary mapping file uuids to open file handles and a
        # dictionary mapping temporary file's file uuids to their absolute paths.
        self.file_paths = {}
        self.file_handles = {}
        self.delete_file_paths = {}

    def open_file(self, path):
        '''
        Open a read-only file handle to the given path and return a uuid identifying it.
        '''
        if not os.path.exists(path) or os.path.islink(path):
            # Note: don't follow symlinks!
            return None
        file_uuid = uuid.uuid4().hex
        self.file_paths[file_uuid] = path
        self.file_handles[file_uuid] = open(path, 'rb')
        return file_uuid

    def open_packed_path(self, path):
        '''
        Open a file handle corresponding to streaming the archived version of |path|.
        '''
        if not os.path.exists(path) or os.path.islink(path):
            # Note: don't follow symlinks!
            return None
        file_uuid = uuid.uuid4().hex
        self.file_paths[file_uuid] = path
        self.file_handles[file_uuid] = zip_util.open_packed_path(path, follow_symlinks=False, exclude_patterns=None)
        return file_uuid

    def open_temp_file(self, name):
        '''
        Open a new temp file with given |name| for writing and return a file
        uuid identifying it.  Put the file in a temporary directory so the file
        can have the desired name.
        '''
        base_path = tempfile.mkdtemp('-file_server_open_temp_file')
        path = os.path.join(base_path, name)
        file_uuid = uuid.uuid4().hex
        self.file_paths[file_uuid] = path
        self.file_handles[file_uuid] = open(path, 'wb')
        self.delete_file_paths[file_uuid] = base_path
        return file_uuid

    def read_file(self, file_uuid, num_bytes=None):
        '''
        Read up to num_bytes from the given file uuid. Return an empty buffer
        if and only if this file handle is at EOF.
        '''
        file_handle = self.file_handles[file_uuid]
        return xmlrpclib.Binary(file_handle.read(num_bytes))

    def readline_file(self, file_uuid):
        '''
        Read one line from the given file uuid. Return an empty buffer
        if and only if this file handle is at EOF.
        '''
        file_handle = self.file_handles[file_uuid]
        return xmlrpclib.Binary(file_handle.readline());

    def seek_file(self, file_uuid, offset, whence):
        '''
        Go to the desired position.
        '''
        file_handle = self.file_handles[file_uuid]
        return file_handle.seek(offset, whence)

    def tell_file(self, file_uuid):
        '''
        Return the current file position.
        '''
        file_handle = self.file_handles[file_uuid]
        return file_handle.tell()

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
        file_handle = self.file_handles[file_uuid]
        file_handle.close()

    def finalize_file(self, file_uuid):
        '''
        Remove the record from the file server.
        '''
        path = self.file_paths.pop(file_uuid)
        file_handle = self.file_handles.pop(file_uuid, None)
        path = self.delete_file_paths.pop(file_uuid, None)
        if path:
            path_util.remove(path)

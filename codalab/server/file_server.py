'''
FileServer is an RPC server that exposes a file-like interface for reading and
writing files on the server's local filesystem.

The core method that opens files handles, open_file, is NOT exposed as an RPC
method for security reasons. Instead, alternate methods for opening files (such
as open_temp_file) are exposed by this class and its subclasses. These methods
all return a file uuid, which is like a Unix file descriptor.

The other RPC methods on this server are read_file, write_file, and close_file.
These methods take a file uuid in addition to their regular arguments, and they
perform the requested operation on the file handle corresponding to that uuid.
'''
import os
from SimpleXMLRPCServer import (
    SimpleXMLRPCServer,
    SimpleXMLRPCRequestHandler,
)
import tempfile
import uuid
import xmlrpclib
xmlrpclib.Marshaller.dispatch[int] = lambda _, v, w : w("<value><i8>%d</i8></value>" % v)  # Hack to allow 64-bit integers

from codalab.client.remote_bundle_client import RemoteBundleClient
from codalab.lib import (
  path_util,
  file_util,
)

class AuthenticatedXMLRPCRequestHandler(SimpleXMLRPCRequestHandler):
    """
    Simple XML-RPC request handler class which also reads authentication
    information included in HTTP headers.
    """

    def decode_request_content(self, data):
        '''
        Overrides in order to capture Authorization header.
        '''
        token = None
        if 'Authorization' in self.headers:
            value = self.headers.get("Authorization", "")
            token = value[8:] if value.startswith("Bearer: ") else ""
        if self.server.auth_handler.validate_token(token):
            return SimpleXMLRPCRequestHandler.decode_request_content(self, data)
        else:
            self.send_response(401, "Could not authenticate with OAuth")
            self.send_header("WWW-Authenticate", "realm=\"https://www.codalab.org\"")
            self.send_header("Content-length", "0")
            self.end_headers()

    def send_response(self, code, message=None):
        '''
        Overrides to capture end of request.
        '''
        # Clear current user
        self.server.auth_handler.validate_token(None)
        SimpleXMLRPCRequestHandler.send_response(self, code, message)

# Turn on multi-threading
import SocketServer
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass
class FileServer(AsyncXMLRPCServer):
    FILE_SUBDIRECTORY = 'file'

    def __init__(self, address, temp, auth_handler):
        # Keep a dictionary mapping file uuids to open file handles and a
        # dictionary mapping temporary file's file uuids to their absolute paths.
        self.file_paths = {}
        self.file_handles = {}
        self.temp = temp
        self.auth_handler = auth_handler
        # Register file-like RPC methods to allow for file transfer.

        SimpleXMLRPCServer.__init__(self, address, allow_none=True,
                                    requestHandler=AuthenticatedXMLRPCRequestHandler,
                                    logRequests=(self.verbose >= 1))
        def wrap(command, func):
            def inner(*args, **kwargs):
                if self.verbose >= 1:
                    print "file_server: %s %s" % (command, args)
                return func(*args, **kwargs)
            return inner
        for command in RemoteBundleClient.FILE_COMMANDS:
            self.register_function(wrap(command, getattr(self, command)), command)

    def open_file(self, path):
        '''
        Open a read-only file handle to the given path and return a uuid identifying it.
        '''
        return self._open_file(path, 'rb')

    def _open_file(self, path, mode):
        '''
        Open a file handleto the given path with the given mode and return a uuid identifying it.
        Should not be used directly, as opening non-temp files for writing can cause race conditions.
        '''
        if os.path.exists(path):
            file_uuid = uuid.uuid4().hex
            self.file_paths[file_uuid] = path
            self.file_handles[file_uuid] = open(path, mode)
            return file_uuid
        return None

    def open_temp_file(self):
        '''
        Open a new temp file for write and return a file uuid identifying it.
        '''
        (fd, path) = tempfile.mkstemp(dir=self.temp)
        os.close(fd)
        return self._open_file(path, 'wb')

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

    def finalize_file(self, file_uuid, delete):
        '''
        Remove the record from the file server.
        '''
        path = self.file_paths.pop(file_uuid)
        file_handle = self.file_handles.pop(file_uuid, None)
        if delete and path: path_util.remove(path)
        #print "SHOULD BE SMALL:", self.file_paths, self.file_handles

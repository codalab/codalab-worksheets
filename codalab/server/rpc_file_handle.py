'''
RPCFileHandle is a wrapper class that takes a file uuid and a proxy for the
FileServer that provided that file uuid. This wrapper provides a very simple
file-like interface for that file handle.
'''
import xmlrpclib


class RPCFileHandle(object):
    def __init__(self, file_uuid, proxy):
        self.file_uuid = file_uuid
        self.proxy = proxy
        self.closed = False

    def read(self, num_bytes=None):
        return self.proxy.read_file(self.file_uuid, num_bytes).data

    def seek(self, offset, whence):
        return self.proxy.seek_file(self.file_uuid, offset, whence)

    def tell(self):
        return self.proxy.tell_file(self.file_uuid)

    def readline(self):
        return self.proxy.readline_file(self.file_uuid).data

    def write(self, buffer):
        binary = xmlrpclib.Binary(buffer)
        self.proxy.write_file(self.file_uuid, binary)

    def close(self):
        if not self.closed:
            self.proxy.close_file(self.file_uuid)
            self.closed = True

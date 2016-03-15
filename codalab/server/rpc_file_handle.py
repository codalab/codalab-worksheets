'''
RPCFileHandle is a wrapper class that takes a file uuid and a proxy for the
FileServer that manages that file uuid. This wrapper provides a very simple
file-like interface for reading and writing to that file handle.
'''
import xmlrpclib


class RPCFileHandle(object):
    def __init__(self, file_uuid, proxy, finalize_on_close=False):
        self.file_uuid = file_uuid
        self.proxy = proxy
        self.finalize_on_close = finalize_on_close
        self.closed = False

    def read(self, num_bytes=None):
        return self.proxy.read_file(self.file_uuid, num_bytes).data

    def write(self, buffer):
        binary = xmlrpclib.Binary(buffer)
        self.proxy.write_file(self.file_uuid, binary)

    def close(self):
        if not self.closed:
            self.proxy.close_file(self.file_uuid)
            if self.finalize_on_close:
                self.proxy.finalize_file(self.file_uuid)
            self.closed = True

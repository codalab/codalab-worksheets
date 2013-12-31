import xmlrpclib


class RPCFileHandle(object):
  def __init__(self, file_uuid, proxy):
    self.file_uuid = file_uuid
    self.proxy = proxy
    self.closed = False
  
  def read(self, num_bytes=None):
    return self.proxy.read_file(self.file_uuid, num_bytes).data
  
  def write(self, buffer):
    binary = xmlrpclib.Binary(buffer)
    self.proxy.write_file(self.file_uuid, binary)
  
  def flush(self):
    pass
    
  def close(self):
    if not self.closed:
      self.proxy.close_file(self.file_uuid)
      self.closed = True

import cgi
from BaseHTTPServer import (
  BaseHTTPRequestHandler,
  HTTPServer,
)

from codalab.common import BUNDLE_FTP_PORT


class BundleFTPServer(HTTPServer):
  class MultipartHandler(BaseHTTPRequestHandler):
    def do_POST(self):
      header = self.headers.getheader('content-type')
      (content_type, header_dict) = cgi.parse_header(header)
      if content_type != 'multipart/form-data':
        raise TypeError('Unexpected content-type: %s' % (content_type,))
      print self.rfile
      print self.rfile.read
      multipart = cgi.parse_multipart(self.rfile, header_dict)
      print multipart

  def __init__(self):
    address = ('localhost', BUNDLE_FTP_PORT)
    HTTPServer.__init__(self, address, self.MultipartHandler)

  def serve_forever(self):
    print 'FTP server serving on port %s...' % (BUNDLE_FTP_PORT,)
    HTTPServer.serve_forever(self)

from contextlib import closing
from cStringIO import StringIO
import httplib
import json
import urllib
import urllib2
import urlparse

from file_util import un_gzip_stream


class RestClientException(Exception):
    """
    Exception raised by the RestClient methods on error. If
    client_error is False, the failure is caused by a server-side error and
    can be retried.
    """

    def __init__(self, message, client_error):
        super(RestClientException, self).__init__(message)
        self.client_error = client_error


class RestClient(object):
    """
    Generic class for communicating with a REST API authenticated by OAuth.
    """

    def __init__(self, base_url):
        self._base_url = base_url

    def _get_access_token(self):
        """
        Should be overriden with a thread-safe method that returns a valid
        access token.
        """
        raise NotImplementedError

    def _make_request(self, method, path, query_params=None, headers=None,
                      data=None, return_response=False, authorized=True):
        if headers is None:
            headers = {}

        access_token = self._get_access_token()
        if authorized and access_token:
            headers['Authorization'] = 'Bearer ' + self._get_access_token()

        if data is not None and isinstance(data, dict):
            headers['Content-Type'] = 'application/json'
            data = json.dumps(data)
        headers['X-Requested-With'] = 'XMLHttpRequest'
        if query_params is not None:
            path = path + '?' + urllib.urlencode(query_params)
        request = urllib2.Request(self._base_url + path, data=data,
                                  headers=headers)
        request.get_method = lambda: method
        if return_response:
            # Return a file-like object containing the contents of the response
            # body, transparently decoding gzip streams if indicated by the
            # Content-Encoding header.
            response = urllib2.urlopen(request)
            encoding = response.headers.get('Content-Encoding')
            if not encoding or encoding == 'identity':
                return response
            elif encoding == 'gzip':
                return un_gzip_stream(response)
            else:
                raise RestClientException(
                    'Unsupported Content-Encoding: ' + encoding, False)
        with closing(urllib2.urlopen(request)) as response:
            # If the response is a JSON document, as indicated by the
            # Content-Type header, try to deserialize it and return the result.
            # Otherwise, just ignore the response body and return None.
            if response.headers.get('Content-Type') == 'application/json':
                response_data = response.read()
                try:
                    return json.loads(response_data)
                except ValueError:
                    raise RestClientException(
                        'Invalid JSON: ' + response_data, False)

    def _upload_with_chunked_encoding(self, method, url, query_params, fileobj,
                                      progress_callback=None):
        # Start the request.
        parsed_base_url = urlparse.urlparse(self._base_url)
        path = url + '?' + urllib.urlencode(query_params)
        if parsed_base_url.scheme == 'http':
            conn = httplib.HTTPConnection(parsed_base_url.netloc)
        else:
            conn = httplib.HTTPSConnection(parsed_base_url.netloc)
        with closing(conn):
            conn.putrequest(method, parsed_base_url.path + path)

            # Set headers.
            conn.putheader('Authorization', 'Bearer ' + self._get_access_token())
            conn.putheader('Transfer-Encoding', 'chunked')
            conn.putheader('X-Requested-With', 'XMLHttpRequest')
            conn.endheaders()

            # Use chunked transfer encoding to send the data through.
            bytes_uploaded = 0
            while True:
                to_send = fileobj.read(16 * 1024)
                if not to_send:
                    break
                conn.send('%X\r\n%s\r\n' % (len(to_send), to_send))
                bytes_uploaded += len(to_send)
                if progress_callback is not None:
                    progress_callback(bytes_uploaded)
            conn.send('0\r\n\r\n')

            # Read the response.
            response = conn.getresponse()
            if response.status != 200:
                # Low-level httplib module doesn't throw HTTPError
                raise urllib2.HTTPError(
                    self._base_url + path,
                    response.status,
                    response.reason,
                    dict(response.getheaders()),
                    StringIO(response.read()))

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

    """
    A dictionary of additional headers to send along with HTTP requests.
    """
    _extra_headers = {}

    def __init__(self, base_url):
        self._base_url = base_url

    def _get_access_token(self):
        """
        Should be overriden with a thread-safe method that returns a valid
        access token.
        """
        raise NotImplementedError

    def _make_request(
        self,
        method,
        path,
        query_params=None,
        headers=None,
        data=None,
        return_response=False,
        authorized=True,
    ):
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

        # Everything needs to be utf-8 encoded or else urllib2 will complain
        if 'Content-Type' in headers:
            headers['Content-Type'] += '; charset=utf-8'
        if data and isinstance(data, unicode):
            data = data.encode('utf-8')
        request_url = (self._base_url + path).encode('utf-8')

        headers.update(self._extra_headers)

        request = urllib2.Request(request_url, data=data, headers=headers)
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
                raise RestClientException('Unsupported Content-Encoding: ' + encoding, False)
        with closing(urllib2.urlopen(request)) as response:
            # If the response is a JSON document, as indicated by the
            # Content-Type header, try to deserialize it and return the result.
            # Otherwise, just ignore the response body and return None.
            if response.headers.get('Content-Type') == 'application/json':
                response_data = response.read()
                try:
                    return json.loads(response_data)
                except ValueError:
                    raise RestClientException('Invalid JSON: ' + response_data, False)

    def _upload_with_chunked_encoding(
        self, method, url, query_params, fileobj, progress_callback=None
    ):
        """
        Uploads the fileobj to url using method with query_params,
        if progress_callback is specified, it is called with the
        number of bytes uploaded after each chunk upload is finished
        the optional progress_callback should return a boolean which interrupts the
        download if False and resumes it if True. If i's not specified the download
        runs to completion
        """
        CHUNK_SIZE = 16 * 1024
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
            headers = {
                'Authorization': 'Bearer ' + self._get_access_token(),
                'Transfer-Encoding': 'chunked',
                'X-Requested-With': 'XMLHttpRequest',
            }
            headers.update(self._extra_headers)
            for header_name, header_value in headers.iteritems():
                conn.putheader(header_name, header_value)
            conn.endheaders()

            # Use chunked transfer encoding to send the data through.
            bytes_uploaded = 0
            while True:
                to_send = fileobj.read(CHUNK_SIZE)
                if not to_send:
                    break
                conn.send('%X\r\n%s\r\n' % (len(to_send), to_send))
                bytes_uploaded += len(to_send)
                if progress_callback is not None:
                    should_resume = progress_callback(bytes_uploaded)
                    if not should_resume:
                        raise Exception('Upload aborted by client')
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
                    StringIO(response.read()),
                )

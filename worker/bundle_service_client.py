import base64
from contextlib import closing
import httplib
import json
import re
import socket
import sys
import threading
import time
import urllib
import urllib2
import urlparse

from file_util import tar_gzip_directory


def wrap_exception(message):
    def decorator(f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except BundleServiceException as e:
                raise BundleServiceException, \
                    BundleServiceException(message + ': ' + e.message,
                                           e.client_error), \
                    sys.exc_info()[2]
            except urllib2.HTTPError as e:
                raise BundleServiceException, \
                    BundleServiceException(message + ': ' +
                                           httplib.responses[e.code] + ' - ' +
                                           e.read(),
                                           e.code >= 400 and e.code < 500), \
                    sys.exc_info()[2]
            except (urllib2.URLError, httplib.HTTPException, socket.error) as e:
                raise BundleServiceException, \
                    BundleServiceException(message + ': ' + str(e), False), \
                    sys.exc_info()[2]
        return wrapper
    return decorator


class BundleServiceException(Exception):
    """
    Exception raised by the BundleServiceClient methods on error. If
    client_error is False, the failure is caused by a server-side error and
    can be retried.
    """
    def __init__(self, message, client_error):
        super(BundleServiceException, self).__init__(message)
        self.client_error = client_error


def authorized(f):
    def wrapper(bundle_service_client, *args, **kwargs):
        bundle_service_client._check_authorization()
        return f(bundle_service_client, *args, **kwargs)
    return wrapper


class BundleServiceClient(object):
    """
    Methods for calling the bundle service.
    """
    def __init__(self, base_url, username, password):
        self._base_url = base_url + '/rest'
        self._username = username
        self._password = password

        self._authorization_lock = threading.Lock()
        self._access_token = None
        self._token_expiration_time = None

    def _check_authorization(self):
        with self._authorization_lock:
            if (not self._access_token
                or time.time() > self._token_expiration_time - 5 * 60):
                self._authorize()

    @wrap_exception('Unable to authorize with bundle service')
    def _authorize(self):
        request_data = {
            'grant_type': 'password',
            'username': self._username,
            'password': self._password}
        headers = {
            'Authorization': 'Basic ' + base64.b64encode('codalab_worker_client:'),
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest'}
        request = urllib2.Request(
            self._base_url + '/oauth2/token',
            data=urllib.urlencode(request_data),
            headers=headers)
        with closing(urllib2.urlopen(request)) as response:
            response_data = response.read()
        try:
            token = json.loads(response_data)
        except ValueError:
            raise BundleServiceException('Invalid JSON: ' + response_data, False)
        if token['token_type'] != 'Bearer':
            raise BundleServiceException(
                'Unknown authorization token type: ' + token['token_type'], True)
        self._access_token = token['access_token']
        self._token_expiration_time = time.time() + token['expires_in']

    def _worker_url_prefix(self, worker_id):
        return '/worker/' + urllib.quote(worker_id)

    @authorized
    @wrap_exception('Unable to check in with bundle service')
    def checkin(self, worker_id, request_data):
        return self._make_request(
            'POST', self._worker_url_prefix(worker_id) + '/checkin',
            data=request_data)

    @authorized
    @wrap_exception('Unable to check out from bundle service')
    def checkout(self, worker_id):
        return self._make_request(
            'POST', self._worker_url_prefix(worker_id) + '/checkout')

    @authorized
    @wrap_exception('Unable to reply to message from bundle service')
    def reply(self, worker_id, socket_id, message):
        self._make_request(
            'POST', self._worker_url_prefix(worker_id) + '/reply/' + str(socket_id),
            data=message)

    @authorized
    @wrap_exception('Unable to reply to message from bundle service')
    def reply_data(self, worker_id, socket_id, header_message, fileobj_or_string):
        method = 'POST'
        url = self._worker_url_prefix(worker_id) + '/reply_data/' + str(socket_id)
        query_params = {
            'header_message': json.dumps(header_message),
        }
        if isinstance(fileobj_or_string, basestring):
            self._make_request(method, url, query_params,
                               headers={}, data=fileobj_or_string)
        else:
            self._upload_with_chunked_encoding(
                method, url, query_params, fileobj_or_string)

    @authorized
    @wrap_exception('Unable to start bundle in bundle service')
    def start_bundle(self, worker_id, uuid, request_data):
        return self._make_request(
            'POST', self._worker_url_prefix(worker_id) + '/start_bundle/' + uuid,
            data=request_data)

    @authorized
    @wrap_exception('Unable to update bundle metadata in bundle service')
    def update_bundle_metadata(self, worker_id, uuid, new_metadata):
        self._make_request(
            'PUT', self._worker_url_prefix(worker_id) + '/update_bundle_metadata/' + uuid,
            data=new_metadata)

    @authorized
    @wrap_exception('Unable to update bundle contents in bundle service')
    def update_bundle_contents(self, worker_id, uuid, path):
        with closing(tar_gzip_directory(path)) as fileobj:
            self._upload_with_chunked_encoding(
                'PUT', self._worker_url_prefix(worker_id) + '/update_bundle_contents/' + uuid,
                query_params={'filename': 'bundle.tar.gz'}, fileobj=fileobj)

    @authorized
    @wrap_exception('Unable to finalize bundle in bundle service')
    def finalize_bundle(self, worker_id, uuid, request_data):
        self._make_request(
            'POST', self._worker_url_prefix(worker_id) + '/finalize_bundle/' + uuid,
            data=request_data)

    @wrap_exception('Unable to get worker code')
    def get_code(self):
        return self._make_request(
            'GET', '/worker/code.tar.gz', return_response=True, authorized=False)

    @authorized
    @wrap_exception('Unable to get bundle contents from bundle service')
    def get_bundle_contents(self, uuid):
        """
        Returns a file-like object and a file name.
        """
        response = self._make_request(
            'GET', '/bundle/' + uuid + '/contents/blob/',
            headers={'Accept-Encoding': 'gzip'}, return_response=True)
        match = re.match('filename="(.*)"',
                         response.headers['Content-Disposition'])
        return (response, match.group(1))

    def _make_request(self, method, url, query_params=None, headers={}, data=None,
                      return_response=False, authorized=True):
        if authorized:
            with self._authorization_lock:
                headers['Authorization'] = 'Bearer ' + self._access_token
        
        if data is not None and isinstance(data, dict):
            headers['Content-Type'] = 'application/json'
            data = json.dumps(data)
        headers['X-Requested-With'] = 'XMLHttpRequest'
        if query_params is not None:
            url = url + '?' + urllib.urlencode(query_params)
        request = urllib2.Request(self._base_url + url, data=data, headers=headers)
        request.get_method = lambda: method
        if return_response:
            return urllib2.urlopen(request)
        with closing(urllib2.urlopen(request)) as response:
            if response.headers.get('Content-Type') == 'application/json':
                response_data = response.read()
                try:
                    return json.loads(response_data)
                except ValueError:
                    raise BundleServiceException(
                        'Invalid JSON: ' + response_data, False)

    def _upload_with_chunked_encoding(self, method, url, query_params, fileobj):
        # Start the request.
        parsed_base_url = urlparse.urlparse(self._base_url)
        if parsed_base_url.scheme == 'http':
            conn = httplib.HTTPConnection(parsed_base_url.netloc)
        else:
            conn = httplib.HTTPSConnection(parsed_base_url.netloc)
        with closing(conn):
            conn.putrequest(
                method, parsed_base_url.path + url + '?' + urllib.urlencode(query_params))

            # Set headers.
            with self._authorization_lock:
                access_token = self._access_token
            conn.putheader('Authorization', 'Bearer ' + access_token)
            conn.putheader('Transfer-Encoding', 'chunked')
            conn.putheader('X-Requested-With', 'XMLHttpRequest')
            conn.endheaders()

            # Use chunked transfer encoding to send the data through.
            while True:
                to_send = fileobj.read(16 * 1024)
                if not to_send:
                    break
                conn.send('%X\r\n%s\r\n' % (len(to_send), to_send))
            conn.send('0\r\n\r\n')

            # Read the response.
            response = conn.getresponse()
            if response.status != 200:
                raise BundleServiceException(
                    httplib.responses[response.status] + ' - ' + response.read(),
                    response.status >= 400 and response.status < 500)

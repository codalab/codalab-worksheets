import base64
from contextlib import closing
import http.client
import json
import socket
import threading
import time
import urllib.request, urllib.parse, urllib.error

from .rest_client import RestClient, RestClientException
from .file_util import tar_gzip_directory, BINARY_PLACEHOLDER


def wrap_exception(message):
    def decorator(f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except BundleAuthException:
                raise
            except RestClientException as e:
                raise BundleServiceException(message + ': ' + str(e), e.client_error)
            except urllib.error.HTTPError as e:
                try:
                    # Ensure the type of urllib.error.HTTPError response to be string
                    client_error = ensure_str(e.read())
                    if e.reason == 'invalid_grant':
                        raise BundleAuthException(
                            message + ': ' + http.client.responses[e.code] + ' - ' + client_error,
                            True,
                        )
                    else:
                        raise BundleServiceException(
                            message + ': ' + http.client.responses[e.code] + ' - ' + client_error,
                            e.code >= 400 and e.code < 500,
                        )
                except json.decoder.JSONDecodeError as e:
                    raise BundleServiceException(message + ': ' + str(e), False)
            except (urllib.error.URLError, http.client.HTTPException, socket.error) as e:
                raise BundleServiceException(message + ': ' + str(e), False)

        return wrapper

    return decorator


def ensure_str(response):
    """
    Ensure the data type of input response to be string
    :param response: a response in bytes or string
    :return: the input response in string
    """
    if isinstance(response, str):
        return response
    try:
        return response.decode()
    except UnicodeDecodeError:
        return BINARY_PLACEHOLDER


class BundleAuthException(RestClientException):
    """
    Exception raised by the BundleServiceClient methods if auth error occurs.
    """


class BundleServiceException(RestClientException):
    """
    Exception raised by the BundleServiceClient methods on error. If
    client_error is False, the failure is caused by a server-side error and
    can be retried.
    """


class BundleServiceClient(RestClient):
    """
    Methods for calling the bundle service.
    """

    def __init__(self, base_url, username, password):
        self._username = username
        self._password = password

        self._authorization_lock = threading.Lock()
        self._access_token = None
        self._token_expiration_time = None

        base_url += '/rest'
        super(BundleServiceClient, self).__init__(base_url)
        try:
            self._authorize()
        except BundleServiceException as ex:
            raise BundleAuthException(ex, True)

    def _get_access_token(self):
        with self._authorization_lock:
            if not self._access_token or time.time() > self._token_expiration_time - 5 * 60:
                self._authorize()
            return self._access_token

    @wrap_exception('Unable to authorize with bundle service')
    def _authorize(self):
        request_data = {
            'grant_type': 'password',
            'username': self._username,
            'password': self._password,
        }
        headers = {
            'Authorization': 'Basic ' + base64.b64encode(b'codalab_worker_client:').decode('utf-8'),
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest',
        }
        request = urllib.request.Request(
            self._base_url + '/oauth2/token',
            data=urllib.parse.urlencode(request_data).encode('utf-8'),
            headers=headers,
        )
        with closing(urllib.request.urlopen(request)) as response:
            response_data = response.read().decode()
        try:
            token = json.loads(response_data)
        except ValueError:
            raise BundleServiceException('Invalid JSON: ' + response_data, False)
        if token['token_type'] != 'Bearer':
            raise BundleServiceException(
                'Unknown authorization token type: ' + token['token_type'], True
            )
        self._access_token = token['access_token']
        self._token_expiration_time = time.time() + token['expires_in']

    def _worker_url_prefix(self, worker_id):
        return '/workers/' + urllib.parse.quote(worker_id)

    @wrap_exception('Unable to check in with bundle service')
    def checkin(self, worker_id, request_data):
        return self._make_request(
            'POST', self._worker_url_prefix(worker_id) + '/checkin', data=request_data
        )

    @wrap_exception('Unable to reply to message from bundle service')
    def reply(self, worker_id, socket_id, message):
        self._make_request(
            'POST', self._worker_url_prefix(worker_id) + '/reply/' + str(socket_id), data=message
        )

    @wrap_exception('Unable to reply to message from bundle service')
    def reply_data(self, worker_id, socket_id, header_message, fileobj_or_bytestring):
        method = 'POST'
        url = self._worker_url_prefix(worker_id) + '/reply_data/' + str(socket_id)
        query_params = {'header_message': json.dumps(header_message)}
        if isinstance(fileobj_or_bytestring, bytes):
            self._make_request(method, url, query_params, headers={}, data=fileobj_or_bytestring)
        elif isinstance(fileobj_or_bytestring, str):
            raise Exception('Expected bytes, got string')
        else:
            self._upload_with_chunked_encoding(method, url, query_params, fileobj_or_bytestring)

    @wrap_exception('Unable to start bundle in bundle service')
    def start_bundle(self, worker_id, uuid, request_data):
        return self._make_request(
            'POST', self._worker_url_prefix(worker_id) + '/start_bundle/' + uuid, data=request_data
        )

    @wrap_exception('Unable to update bundle contents in bundle service')
    def update_bundle_contents(self, worker_id, uuid, path, progress_callback):
        with closing(tar_gzip_directory(path)) as fileobj:
            self._upload_with_chunked_encoding(
                'PUT',
                '/bundles/' + uuid + '/contents/blob/',
                query_params={'filename': 'bundle.tar.gz', 'finalize_on_success': 0},
                fileobj=fileobj,
                progress_callback=progress_callback,
            )

    @wrap_exception('Unable to get worker code')
    def get_code(self):
        return self._make_request(
            'GET', '/workers/code.tar.gz', return_response=True, authorized=False
        )

    @wrap_exception('Unable to get bundle contents from bundle service')
    def get_bundle_contents(self, uuid, path):
        """
        Returns a file-like object and a file name.
        """
        response = self._make_request(
            'GET',
            '/bundles/' + uuid + '/contents/blob/' + path,
            headers={'Accept-Encoding': 'gzip'},
            return_response=True,
        )
        return response, response.headers.get('Target-Type')

from contextlib import closing
import json
import urllib.request
import urllib.parse
import urllib.error
from typing import Dict

from .file_util import stream_chunks_from_fileobj, un_gzip_stream
from codalab.common import URLOPEN_TIMEOUT_SECONDS, urlopen_with_retry

import requests


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
    _extra_headers: Dict[str, str] = {}

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
        timeout_seconds=URLOPEN_TIMEOUT_SECONDS,
    ):
        """
        `data` can be one of the following:
        - bytes
        - string (text/plain)
        - dict (application/json)
        """
        # Set headers
        if headers is None:
            headers = {}
        headers['X-Requested-With'] = 'XMLHttpRequest'
        access_token = self._get_access_token()
        if authorized and access_token:
            headers['Authorization'] = 'Bearer ' + self._get_access_token()

        if isinstance(data, dict):
            headers['Content-Type'] = 'application/json'
            data = json.dumps(data)  # Turn dict into string
        if isinstance(data, str):
            data = data.encode()  # Turn string into bytes

        # Emphasize utf-8 for non-bytes data.
        if headers.get('Content-Type') in ('text/plain', 'application/json'):
            headers['Content-Type'] += '; charset=utf-8'

        headers.update(self._extra_headers)

        # Set path
        if query_params is not None:
            path = path + '?' + urllib.parse.urlencode(query_params)
        request_url = self._base_url + path

        # Make the actual request
        request = urllib.request.Request(request_url, data=data, headers=headers)
        request.get_method = lambda: method
        if return_response:
            # Return a file-like object containing the contents of the response
            # body, transparently decoding gzip streams if indicated by the
            # Content-Encoding header.
            response = urlopen_with_retry(request, timeout=timeout_seconds)
            encoding = response.headers.get('Content-Encoding')
            if not encoding or encoding == 'identity':
                return response
            elif encoding == 'gzip':
                return un_gzip_stream(response)
            else:
                raise RestClientException('Unsupported Content-Encoding: ' + encoding, False)

        with closing(urlopen_with_retry(request, timeout=timeout_seconds)) as response:
            # If the response is a JSON document, as indicated by the
            # Content-Type header, try to deserialize it and return the result.
            # Otherwise, just ignore the response body and return None.
            if response.headers.get('Content-Type') == 'application/json':
                response_data = response.read().decode()
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
        # Set headers.
        headers = {
            'Authorization': 'Bearer ' + self._get_access_token(),
            'Transfer-Encoding': 'chunked',
            'X-Requested-With': 'XMLHttpRequest',
        }
        headers.update(self._extra_headers)

        # Wrap the fileobj chunk generator with the progress callback
        def wrap_bytes_generator_in_progress_callback(generator, callback=None):
            bytes_generated = 0
            for chunk in generator:
                yield chunk
                bytes_generated += len(chunk)
                if callback is not None:
                    should_resume = callback(bytes_generated)
                    if not should_resume:
                        raise Exception('Upload aborted by client')

        # Start the chunk-encoded request
        # By providing a generator for the response body, requests automatically uses a
        # chunked-encoded request.
        # https://requests.readthedocs.io/en/master/user/advanced/#chunk-encoded-requests
        response = requests.request(
            method,
            self._base_url + url,
            data=wrap_bytes_generator_in_progress_callback(
                stream_chunks_from_fileobj(fileobj), progress_callback
            ),
            headers=headers,
            params=query_params,
            timeout=URLOPEN_TIMEOUT_SECONDS,
        )
        # Raise an exception if the response is bad.
        response.raise_for_status()

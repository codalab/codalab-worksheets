import unittest
from unittest.mock import Mock, patch
from io import StringIO

from codalab.worker.rest_client import RestClient
from codalab.common import AbortedError


class RestClientTest(unittest.TestCase):
    def setUp(self):
        self.client = RestClient("https://base_url")
        self.client._get_access_token = lambda: "access token"

    @patch('codalab.worker.rest_client.requests.request')
    def test_upload_with_chunked_encoding(self, request):
        """Test upload. We check to see what is passed to requests to ensure that it properly
    calls the progress_callback."""
        progress_callback = Mock()
        sample_data = "abcdefg"
        self.client._upload_with_chunked_encoding(
            method="PUT",
            url="url",
            query_params={},
            fileobj=StringIO(sample_data),
            progress_callback=progress_callback,
        )
        request_data = list(request.call_args[1]["data"])
        self.assertEqual(request_data, [sample_data])
        progress_callback.assert_called_once_with(len(sample_data))

    @patch('codalab.worker.rest_client.requests.request')
    def test_upload_with_chunked_encoding_abort_progress(self, request):
        """Test upload, but with a progress_callback that aborts the request by returning False."""
        progress_callback = lambda _: False
        self.client._upload_with_chunked_encoding(
            method="PUT",
            url="url",
            query_params={},
            fileobj=StringIO("abcdefg"),
            progress_callback=progress_callback,
        )
        with self.assertRaises(AbortedError):
            list(request.call_args[1]["data"])

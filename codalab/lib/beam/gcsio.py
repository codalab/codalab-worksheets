# Monkey-patch init function of GcsIO
from apache_beam.io.gcp.gcsio import GcsIO
import apache_beam.io.gcp.gcsio
from apache_beam.io.gcp.internal.clients import storage
from apache_beam.internal.gcp import auth
from apache_beam.internal.http_client import get_new_http

from google.auth.credentials import AnonymousCredentials

def new_init(self, storage_client=None):
    # raise Exception("This is a test")
    if storage_client is None:
        storage_client = storage.StorageV1(
            url = "http://0.0.0.0:4443/storage/v1/",
            credentials=auth.get_service_credentials(),
            get_credentials=False,
            http=get_new_http(),
            response_encoding='utf8'
        )
    self.client = storage_client
    self._rewrite_cb = None
    self.bucket_to_project_number = {}


# Monkey Patch the GcsIO to upload
apache_beam.io.gcp.gcsio.GcsIO.__init__ = new_init

# class GcsUploader(Uploader):
#     def __init__(self, client, path, mime_type, get_project_number):
#         raise Exception("This is a test")
#     def _start_upload(self):
#         raise Exception("This is a test")

# apache_beam.io.gcsio.GcsUploader = GcsUploader


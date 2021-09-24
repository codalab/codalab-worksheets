from apache_beam.io.filesystemio import Uploader
from azure.storage.blob import (
    ContentSettings,
    BlobBlock,
)
from apache_beam.io.azure.blobstorageio import parse_azfs_path
import base64
from codalab.worker.un_gzip_stream import BytesBuffer

class BlobStorageUploader(Uploader):
  """An improved version of apache_beam.io.azure.blobstorageio.BlobStorageUploader
  that handles multipart streaming (block-by-block) uploads.
  TODO (Ashwin): contribute this back upstream to Apache Beam (https://github.com/codalab/codalab-worksheets/issues/3475).
  """
  # Note that Blob Storage currently can hold a maximum of 100,000 uncommitted blocks.
  # This means that with this current implementation, we can upload a file with a maximum
  # size of 10 TiB to Blob Storage. To exceed that limit, we must either increase MIN_WRITE_SIZE
  # or modify the implementation of this class to call commit_block_list more often (and not
  # just at the end of the upload). 
  MIN_WRITE_SIZE = 100 * 1024 * 1024
  # Maximum block size is 4000 MiB (https://docs.microsoft.com/en-us/rest/api/storageservices/put-block#remarks).
  MAX_WRITE_SIZE = 4000 * 1024 * 1024

  def __init__(self, client, path, mime_type='application/octet-stream'):
    self._client = client
    self._path = path
    self._container, self._blob = parse_azfs_path(path)
    self._content_settings = ContentSettings(mime_type)

    self._blob_to_upload = self._client.get_blob_client(
        self._container, self._blob)

    self.block_number = 1
    self.buffer = BytesBuffer()
    self.block_list = []

  def put(self, data):
    self.buffer.write(data.tobytes())

    while len(self.buffer) >= BlobStorageUploader.MIN_WRITE_SIZE:
      # Take the first chunk off the buffer and write it to Blob Storage
      chunk = self.buffer.read(BlobStorageUploader.MAX_WRITE_SIZE)
      self._write_to_blob(chunk)

  def _write_to_blob(self, data):
    # block_id's have to be base-64 strings normalized to have the same length.
    block_id = base64.b64encode('{0:-32d}'.format(self.block_number).encode()).decode()
    
    self._blob_to_upload.stage_block(block_id, data)
    self.block_list.append(BlobBlock(block_id))
    self.block_number = self.block_number + 1

  def finish(self):
    # The buffer will have a size smaller than MIN_WRITE_SIZE, so its contents can fit into memory.
    self._write_to_blob(self.buffer.read())
    self._blob_to_upload.commit_block_list(self.block_list, content_settings=self._content_settings)

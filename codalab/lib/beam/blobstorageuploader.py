from apache_beam.io.filesystemio import Uploader, UploaderStream
from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings,
    BlobBlock,
)
from apache_beam.io.azure.blobstorageio import parse_azfs_path
import tempfile
import io
import base64

class BlobStorageUploader(Uploader):
  def __init__(self, client, path, mime_type='application/octet-stream'):
    self._client = client
    self._path = path
    self._container, self._blob = parse_azfs_path(path)
    self._content_settings = ContentSettings(mime_type)
    

    self._blob_to_upload = self._client.get_blob_client(
        self._container, self._blob)

    self.block_number = 1
    self.buffer = b''
    self.block_list = []

  def put(self, data):

    # Note that Blob Storage currently can hold a maximum of 100,000 uncommitted blobs.
    # This means that with this current implementation, we can upload a file with a maximum
    # size of 10 TiB to Blob Storage. To exceed that limit, we must either increase MIN_WRITE_SIZE
    # or modify the implementation of this class to call commit_block_list more often (and not
    # just at the end of the upload).
    MIN_WRITE_SIZE = 100 * 1024 * 1024
    MAX_WRITE_SIZE = 2 * 1024 * 1024 * 1024

    # TODO: Byte strings might not be the most performant way to handle this
    self.buffer += data.tobytes()

    while len(self.buffer) >= MIN_WRITE_SIZE:
      # Take the first chunk off the buffer and write it to S3
      chunk = self.buffer[:MAX_WRITE_SIZE]
      self._write_to_blob(chunk)
      # Remove the written chunk from the buffer
      self.buffer = self.buffer[MAX_WRITE_SIZE:]

  def _write_to_blob(self, data):
    # block_id's have to be base-64 strings normalized to have the same length.
    block_id = base64.b64encode('{0:-32d}'.format(self.block_number).encode()).decode()
    
    self._blob_to_upload.stage_block(block_id, data)
    self.block_list.append(BlobBlock(block_id))
    self.block_number = self.block_number + 1

  def finish(self):
    self._write_to_blob(self.buffer)
    self._blob_to_upload.commit_block_list(self.block_list, content_settings=self._content_settings)

"""
This code is just a quick test / sanity check for BlobStorageUploader that can be
run from the command line. To run it, first set the LOCAL_PATH variable
to a local path to a > 100 MB file, then run:
    npm i -g azurite
    azurite
    python codalab/lib/beam/blobstorageuploader.py
"""

if __name__ == '__main__':
    from azure.core.exceptions import ResourceExistsError
    from apache_beam.io.azure.blobstorageio import BlobStorageIO
    import filecmp
    import os
    import shutil
    
    os.environ["CODALAB_AZURE_BLOB_CONNECTION_STRING"] = (
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
    )
    from codalab.lib.beam.filesystems import client
    try:
        client.create_container("bundles")
        print("Created initial Azure Blob Storage container \"bundles\".")
    except ResourceExistsError:
        pass
    LOCAL_PATH = '/Users/epicfaace/Downloads/googlechrome.dmg'
    BLOB_PATH = "azfs://devstoreaccount1/bundles/test.txt"
    uploader = BlobStorageUploader(client, BLOB_PATH)
    with io.BufferedWriter(UploaderStream(uploader, mode="wb"), buffer_size=128 * 1024) as f, open(LOCAL_PATH, 'rb') as inp_f:
        shutil.copyfileobj(inp_f, f)
    with BlobStorageIO(client).open(BLOB_PATH) as f, tempfile.NamedTemporaryFile() as tf:
        shutil.copyfileobj(f, tf)
        result = filecmp.cmp(LOCAL_PATH, tf.name)
        assert result == True
        print("done")
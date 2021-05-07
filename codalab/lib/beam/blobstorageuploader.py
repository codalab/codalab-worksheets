from apache_beam.io.filesystemio import Uploader, UploaderStream
from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings,
)
from apache_beam.io.azure.blobstorageio import parse_azfs_path
import tempfile
import io

class BlobStorageUploader(Uploader):
  def __init__(self, client, path, mime_type='application/octet-stream'):
    self._client = client
    self._path = path
    self._container, self._blob = parse_azfs_path(path)
    self._content_settings = ContentSettings(mime_type)

    self._blob_to_upload = self._client.get_blob_client(
        self._container, self._blob)

    # Temporary file.
    self._temporary_file = tempfile.NamedTemporaryFile()

  def put(self, data):
    print("put", len(data), "bytes")
    self._temporary_file.write(data.tobytes())

  def finish(self):
    print("finally uploading blob")
    self._temporary_file.seek(0)
    # The temporary file is deleted immediately after the operation.
    with open(self._temporary_file.name, "rb") as f:
      self._blob_to_upload.upload_blob(
          f.read(), overwrite=True, content_settings=self._content_settings)

# To run test, run:
# python codalab/lib/beam/blobstorageuploader.py

if __name__ == '__main__':
    from azure.core.exceptions import ResourceExistsError
    from codalab.lib.beam.filesystems import client
    import shutil
    try:
        client.create_container("bundles")
        print("Created initial Azure Blob Storage container \"bundles\".")
    except ResourceExistsError:
        pass
    PATH = '/Users/epicfaace/Downloads/googlechrome.dmg'
    BLOB_PATH = "azfs://devstoreaccount1/bundles/test.txt"
    uploader = BlobStorageUploader(client, BLOB_PATH)
    with io.BufferedWriter(UploaderStream(uploader, mode="wb"), buffer_size=128 * 1024) as f, open(PATH, 'rb') as inp_f:
        shutil.copyfileobj(inp_f, f)

    from apache_beam.io.azure.blobstorageio import BlobStorageIO
    import filecmp
    with BlobStorageIO(client).open(BLOB_PATH) as f, tempfile.NamedTemporaryFile() as tf:
        shutil.copyfileobj(f, tf)
        result = filecmp.cmp(PATH, tf.name)
        print(result)
from io import BytesIO, BufferedReader, UnsupportedOperation
import io
import os
import shutil
import tempfile
from threading import Lock, Thread

from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from typing import Any, Dict, Union, Tuple, IO, cast
from contextlib import closing

from codalab.common import UsageError, StorageType, urlopen_with_retry, parse_linked_bundle_url
from codalab.worker.file_util import OpenFile, tar_gzip_directory, GzipStream
from codalab.worker.bundle_state import State
from codalab.lib import file_util, path_util, zip_util
from codalab.objects.bundle import Bundle
from codalab.lib.zip_util import ARCHIVE_EXTS_DIR
from codalab.lib.print_util import FileTransferProgress
from codalab.worker.un_gzip_stream import BytesBuffer

import indexed_gzip
from ratarmountcore import SQLiteIndexedTar


file_path = 'mkdocs.yml'

class FileStream(BytesIO):
    NUM_READERS = 2
    EXTRA_BUFFER_SIZE = 1024
    def __init__(self, fileobj):
        self._bufs = [BytesBuffer(extra_buffer_size=self.EXTRA_BUFFER_SIZE) for _ in range(0, self.NUM_READERS)]
        self._pos = [0 for _ in range(0, self.NUM_READERS)]
        self._fileobj = fileobj
        self._lock = Lock()  # lock to ensure one does not concurrently read self._fileobj / write to the buffers.
        
        class FileStreamReader(BytesIO):
            def __init__(s, index):
                s._index = index
            
            def read(s, num_bytes=None):
                return self.read(s._index, num_bytes)
            
            def seek(s, *args, **kwargs):
                return self.seek(s._index, *args, **kwargs)

            def tell(s):
                return self.tell(s._index)
        
        self.readers = [FileStreamReader(i) for i in range(0, self.NUM_READERS)]


    def read(self, index: int, num_bytes=None):
        """Read the specified number of bytes from the associated file.
        index: index that specifies which reader is reading.
        """
        with self._lock:
            while num_bytes is None or len(self._bufs[index]) < num_bytes:
                s = self._fileobj.read(num_bytes)
                if not s:
                    break
                for i in range(0, self.NUM_READERS):
                    self._bufs[i].write(s)
        if num_bytes is None:
            num_bytes = len(self._bufs[index])
        s = self._bufs[index].read(num_bytes)
        self._pos[index] += len(s)
        return s

    
    def seek(self, index: int, *args, **kwargs):
        return self._bufs[index].seek(*args, **kwargs)

    def tell(self, index: int):
        return self._bufs[index].tell()

    def close(self):
        self.__input.close()

def upload(file_path, bundle_path = 'azfs://devstoreaccount1/bundles/0x1234/contents.gz'):
    source_fileobj = open(file_path, 'rb')
    output_fileobj = GzipStream(source_fileobj)
    CHUNK_SIZE = 4 * 1024

    TEST_CONN_STR = (
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://localhost:10000/devstoreaccount1;"
    )

    os.environ['AZURE_STORAGE_CONNECTION_STRING'] = TEST_CONN_STR

    # stream_file = tempfile.NamedTemporaryFile(suffix=".gz")
    stream_file = FileStream(output_fileobj)
    reader1 = stream_file.readers[0]
    reader2 = stream_file.readers[1]
    
    def upload_file():
        print("Upload file")
        bytes_uploaded = 0
        with FileSystems.create(
            bundle_path, compression_type=CompressionTypes.UNCOMPRESSED
        ) as out:
            while True:
                to_send = reader1.read(CHUNK_SIZE)
                if not to_send:
                    break
                out.write(to_send)
                bytes_uploaded += len(to_send)

    def create_index():
        print("Create index")
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmp_index_file:
            SQLiteIndexedTar(
                fileObject=reader2,
                tarFileName="contents",  # If saving a single file as a .gz archive, this file can be accessed by the "/contents" entry in the index.
                writeIndex=True,
                clearIndexCache=True,
                indexFilePath=tmp_index_file.name,
                isGnuIncremental=False,
            )

            bytes_uploaded = 0
            with FileSystems.create(
                parse_linked_bundle_url(bundle_path).index_path,
                compression_type=CompressionTypes.UNCOMPRESSED,
            ) as out_index_file, open(tmp_index_file.name, "rb") as tif:
                while True:
                    to_send = tif.read(CHUNK_SIZE)
                    if not to_send:
                        break
                    out_index_file.write(to_send)
                    bytes_uploaded += len(to_send)

    threads = [
        Thread(target=upload_file),
        Thread(target=create_index)
    ]

    for thread in threads:
        thread.start()
    
    for thread in threads:
        thread.join()

    with OpenFile(bundle_path) as f:
        print(f.read())


upload(file_path)
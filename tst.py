from io import BytesIO, BufferedReader
import os
import shutil
import tempfile
from threading import Lock, Thread

from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from typing import Any, Dict, Union, Tuple, IO, cast
from contextlib import closing

from codalab.common import UsageError, StorageType, urlopen_with_retry, parse_linked_bundle_url
from codalab.worker.file_util import tar_gzip_directory, GzipStream
from codalab.worker.bundle_state import State
from codalab.lib import file_util, path_util, zip_util
from codalab.objects.bundle import Bundle
from codalab.lib.zip_util import ARCHIVE_EXTS_DIR
from codalab.lib.print_util import FileTransferProgress
from codalab.worker.un_gzip_stream import BytesBuffer

import indexed_gzip
from codalab.lib.beam.SQLiteIndexedTar import SQLiteIndexedTar


class FileStream(BytesIO):
    NUM_READERS = 2

    def __init__(self, fileobj):
        self._bufs = [BytesBuffer() for _ in range(0, self.NUM_READERS)]
        self._pos = [0 for _ in range(0, self.NUM_READERS)]
        self._fileobj = fileobj
        self._lock = (
            Lock()
        )  # lock to ensure one does not concurrently read self._fileobj / write to the buffers.

        class FileStreamReader(BytesIO):
            def __init__(s, index):
                s._index = index

            def read(s, num_bytes=None):
                return self.read(s._index, num_bytes)

            def peek(s, num_bytes):
                return self.peek(s._index, num_bytes)

        self.readers = [FileStreamReader(i) for i in range(0, self.NUM_READERS)]

    def _fill_buf_bytes(self, index: int, num_bytes=None):
        with self._lock:
            while num_bytes is None or len(self._bufs[index]) < num_bytes:
                s = self._fileobj.read(num_bytes)
                if not s:
                    break
                for i in range(0, self.NUM_READERS):
                    self._bufs[i].write(s)

    def read(self, index: int, num_bytes=None):
        """Read the specified number of bytes from the associated file.
        index: index that specifies which reader is reading.
        """
        self._fill_buf_bytes(index, num_bytes)
        if num_bytes is None:
            num_bytes = len(self._bufs[index])
        s = self._bufs[index].read(num_bytes)
        self._pos[index] += len(s)
        return s

    def peek(self, index: int, num_bytes):
        self._fill_buf_bytes(index, num_bytes)
        s = self._bufs[index].peek(num_bytes)
        return s

    def close(self):
        self.__input.close()


def upload(
    file_path, is_dir=False, bundle_path='azfs://devstoreaccount1/bundles/0x1234/contents.gz'
):
    if is_dir:
        source_fileobj = zip_util.tar_gzip_directory(file_path)
    else:
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
        with FileSystems.create(bundle_path, compression_type=CompressionTypes.UNCOMPRESSED) as out:
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
                printDebug=3,
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

    threads = [Thread(target=upload_file), Thread(target=create_index)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    import gzip

    with FileSystems.open(
        parse_linked_bundle_url(bundle_path).bundle_path,
        compression_type=CompressionTypes.UNCOMPRESSED,
    ) as f:
        # print(gzip.decompress(f.read()))
        pass

    if is_dir:
        linked_bundle_path = parse_linked_bundle_url(bundle_path + '/dir1/f1')
        print(linked_bundle_path.archive_subpath)
        from codalab.worker.file_util import OpenIndexedArchiveFile
        from ratarmountcore import FileInfo

        with OpenIndexedArchiveFile(linked_bundle_path.bundle_path) as tf:
            # listdir = lambda path: cast(Dict[str, FileInfo], tf.listDir(path) or {})
            # print(listdir)
            # info = tf.getFileInfo(linked_bundle_path.archive_subpath)
            info = tf.listDir('/')
            print(info)  # here info is none


file_path = 'dir1'
upload(file_path, is_dir=True)

# import gzip

# # file_path = 'requirements.txt'
# file_path = 'test_10g'

# def test_indexed_gzip(file_path):
#     """
#     A simple test function only envolve SQLiteIndexedTar
#     """
#     source_fileobj = open(file_path, 'rb')
#     # # build_full_index() at line 1447 (in SQLiteIndexedTar.py) does not work for GzipStream()
#     # output_fileobj = GzipStream(BytesIO(source_fileobj.read()))
#     output_fileobj = GzipStream(source_fileobj)

#     # # build_full_index() at line 1447 (in SQLiteIndexedTar.py) works
#     # output_fileobj = BytesIO(gzip.compress(source_fileobj.read()))
#     # def new_seek(*args, **kwargs):
#     #     raise OSError("Seek ERROR")
#     # def new_tell(*args, **kwargs):
#     #     raise OSError("Tell() ERROR")
#     # old_seek = output_fileobj.seek
#     # old_tell = output_fileobj.tell
#     # output_fileobj.seekable = lambda: False
#     # output_fileobj.seek = new_seek
#     # output_fileobj.tell = new_tell

#     with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmp_index_file:
#         SQLiteIndexedTar(
#             fileObject=output_fileobj,
#             tarFileName="contents",  # If saving a single file as a .gz archive, this file can be accessed by the "/contents" entry in the index.
#             writeIndex=True,
#             clearIndexCache=True,
#             indexFilePath=tmp_index_file.name,
#             printDebug=3,
#         )

# test_indexed_gzip(file_path)  # filepath points to a large file.

# # file_path = 'requirements.txt'
# # def simple_test(file_path):
# #     source_fileobj = open(file_path, 'rb')
# #     # output_fileobj = GzipStream(source_fileobj)
# #     output_fileobj = GzipStream(BytesIO(source_fileobj.read()))
# #     tar_file = indexed_gzip.IndexedGzipFile(fileobj=output_fileobj, drop_handles=False, spacing=4194304)
# #     tar_file.build_full_index()

# # simple_test(file_path)

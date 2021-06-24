#!/usr/bin/env python


import os
import errno
import stat
import time
from collections import OrderedDict

from contextlib import closing
from typing import Dict

from codalab.worker.download_util import BundleTarget

try:
    from fuse import FUSE, FuseOSError, Operations

    fuse_is_available = True
except EnvironmentError:
    fuse_is_available = False

if fuse_is_available:
    from codalab.common import NotFoundError

    class ByteRangeReader(object):
        '''
        Manages Byte Ranges for BundleFuse and operates like a cache, fetching via the client
        to refresh or obtain new byte ranges through the REST api whenever it needs to.
        One reader for one bundle at a time (same as BundleFuse)
        '''

        def __init__(self, client, bundle_uuid, timeout=60, max_num_chunks=100, chunk_size=None):
            if chunk_size is None:
                chunk_size = 1 * 1000 * 1000  # 1 MB

            self.chunk_size = chunk_size
            self.max_num_chunks = max_num_chunks
            self.timeout = timeout
            self.client = client
            self.bundle_uuid = bundle_uuid
            self.cache = OrderedDict()  # (path, chunk_id) -> (time, bytearray)

        def read(self, path, length, offset):
            start_offset = offset
            end_offset = offset + length - 1

            start_chunk = self._get_chunk_id(start_offset)
            end_chunk = self._get_chunk_id(end_offset)
            arr = ''
            for chunk_id in range(start_chunk, end_chunk + 1):
                arr += self._fetch_chunk(path, chunk_id)
            return arr[
                offset
                - start_chunk * self.chunk_size : offset
                - start_chunk * self.chunk_size
                + length
            ]

        def _fetch_chunk(self, path, chunk_id):
            '''
            Fetch and return a chunk from the cache, or with the client as necessary.
            Refreshes chunks that are older than self.timeout
            Only save full chunks to the cache.
            Partial chunks (i.e. at the end of a file) are not cached because they could grow.
            '''

            now = int(time.time())
            key = (path, chunk_id)

            if key in self.cache:
                t, arr = self.cache[key]
                if now - t < self.timeout:
                    return arr  # return chunk from cache
                else:
                    self.cache.pop(key)  # pop out expired entry

            # grab from client
            byte_range = (
                chunk_id * self.chunk_size,
                chunk_id * self.chunk_size + self.chunk_size - 1,
            )
            with closing(
                self.client.fetch_contents_blob(BundleTarget(self.bundle_uuid, path), byte_range)
            ) as contents:
                arr = contents.read()

            if len(arr) == self.chunk_size:  # only cache if fetched full chunk
                if len(self.cache) >= self.max_num_chunks:  # if full, remove the oldest item
                    self.cache.popitem(last=False)
                self.cache[key] = (now, arr)

            return arr

        def _get_chunk_id(self, offset):
            ''' Return chunk id given offset '''
            return offset // self.chunk_size

    class MWT(object):
        """
        Memoize With Timeout
        https://code.activestate.com/recipes/325905-memoize-decorator-with-timeout/
        """

        _caches: Dict[int, Dict[int, Dict[int, float]]] = {}
        _timeouts: Dict[int, float] = {}

        def __init__(self, timeout=2):
            self.timeout = timeout

        def collect(self):
            """Clear cache of results which have timed out"""
            for func in self._caches:
                cache = {}
                for key in self._caches[func]:
                    if (time.time() - self._caches[func][key][1]) < self._timeouts[func]:
                        cache[key] = self._caches[func][key]
                self._caches[func] = cache

        def __call__(self, f):
            self.cache = self._caches[f] = {}
            self._timeouts[f] = self.timeout

            def func(*args, **kwargs):
                kw = kwargs.items()
                kw.sort()
                key = (args, tuple(kw))
                try:
                    v = self.cache[key]
                    if (time.time() - v[1]) > self.timeout:
                        raise KeyError
                except KeyError:
                    v = self.cache[key] = f(*args, **kwargs), time.time()
                return v[0]

            func.__name__ = f.__name__

            return func

    class Memoize(MWT):
        '''A superset of MWT that adds the possibility to yank paths from cached results'''

        def yank_path(self, path):
            """Clear cache of results from a specific path"""
            for func in self._caches:
                for key in self._caches[func].keys():
                    if path in key[0]:
                        del self._caches[func][key]

    class BundleFuse(Operations):
        """
        A FUSE filesystem implementation for mounting CodaLab bundles

        This is meant to be a read-only filesystem and so all write functionality is omitted.

        If the bundle is a single file, the mountpoint will look like a directory that contains that single file.

        """

        def __init__(self, client, bundle_uuid, verbose=False):
            self.client = client
            self.bundle_uuid = bundle_uuid
            self.fd = 0  # file descriptor
            self.verbose = verbose
            self.bundle_metadata = self.client.fetch('bundles', self.bundle_uuid)['metadata']

            self.single_file_bundle = False
            self.reader = ByteRangeReader(self.client, self.bundle_uuid)
            info = self._get_info('/')
            if info['type'] == 'file':
                self.single_file_bundle = True
                print(
                    'Note: specified bundle is a single file; mountpoint will look like a directory that contains that single file.'
                )

        # Helpers
        # =======

        @Memoize(timeout=5)
        def _get_info(self, path):
            ''' Set a request through the json api client to get info about the bundle '''
            try:
                info = self.client.fetch_contents_info(BundleTarget(self.bundle_uuid, path), 1)
            except NotFoundError:
                raise FuseOSError(errno.ENOENT)
            return info

        def verbose_print(self, msg):
            if self.verbose:
                print('[BundleFUSE]:', msg)

        # Filesystem methods
        # ==================

        def getattr(self, path, fh=None):
            ''' Fetch standard filesystem attributes '''

            if self.single_file_bundle:
                info = self._get_info('/')
                if path == '/':
                    mode = stat.S_IFDIR | info['perm']
                    nlink = 2
                else:
                    mode = stat.S_IFREG | info['perm']
                    nlink = 1

            # set item mode and nlinks according to item type
            else:
                info = self._get_info(path)
                if info['type'] == 'directory':
                    mode = stat.S_IFDIR | info['perm']
                    nlink = 2
                elif info['type'] == 'file':
                    mode = stat.S_IFREG | info['perm']
                    nlink = 1
                elif info['type'] == 'link':
                    mode = stat.S_IFLNK | info['perm']
                    nlink = 1

            bundle_created_time = self.bundle_metadata['created']

            attributes = {
                'st_atime': bundle_created_time,
                'st_ctime': bundle_created_time,
                'st_gid': 0,
                'st_mode': mode,
                'st_mtime': bundle_created_time,
                'st_nlink': nlink,
                'st_uid': 0,
                'st_size': info['size'],
            }

            self.verbose_print('getattr path={}, attr={}'.format(path, attributes))
            return attributes

        def readdir(self, path, fh):
            ''' Yield a sequence of entries in the filesystem under the current path '''
            dirents = ['.', '..']

            if self.single_file_bundle:
                dirents.append(self.bundle_metadata['name'])
            else:
                info = self._get_info(path)
                items = info.get('contents', [])
                for d in items:
                    dirents.append(d['name'])

            self.verbose_print('readdir path={}, dirents={}'.format(path, dirents))

            for r in dirents:
                yield r

        def readlink(self, path):
            ''' Figure out where the link points to '''

            if self.single_file_bundle:
                path = '/'

            info = self._get_info(path)

            pathname = info['link']
            self.verbose_print('readlink path={}, pathname={}'.format(path, pathname))

            # TODO: not sure if this is completely correct
            if pathname.startswith("/"):
                # Path name is absolute, sanitize it.
                return os.path.relpath(pathname, self.root)
            else:
                return pathname

        # File methods
        # ============

        def open(self, path, flags):
            '''
            Open a file, return a file descriptor.
            (fabianc: This seems to be the way people do it, I have no idea why it's done this way.)
            '''

            self.fd += 1
            self.verbose_print('open path={}'.format(path))
            return self.fd

        def read(self, path, length, offset, fh):
            ''' Return a range of bytes from a path as specified.  '''

            if self.single_file_bundle:
                path = '/'

            result = self.reader.read(path, length, offset)

            self.verbose_print('read path={}, length={}, offset={}'.format(path, length, offset))
            return result

    def bundle_mount(client, mountpoint, bundle_uuid, verbose=False):
        ''' Mount the filesystem on the mountpoint. '''
        FUSE(BundleFuse(client, bundle_uuid, verbose), mountpoint, nothreads=True, foreground=True)

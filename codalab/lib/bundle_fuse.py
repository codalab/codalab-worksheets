#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import stat
import time

from contextlib import closing

try:
    from fuse import FUSE, FuseOSError, Operations
    fuse_is_available = True
except EnvironmentError:
    fuse_is_available = False

if fuse_is_available:
    from codalab.client.json_api_client import JsonApiRelationship
    from codalab.lib.path_util import normalize

    class MWT(object):
        """
        Memoize With Timeout
        https://code.activestate.com/recipes/325905-memoize-decorator-with-timeout/
        """

        _caches = {}
        _timeouts = {}

        def __init__(self,timeout=2):
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
                    v = self.cache[key] = f(*args,**kwargs),time.time()
                return v[0]
            func.func_name = f.func_name

            return func

    class Memoize(MWT):
        '''A superset of MWT that adds the possibility to yank paths from cached results'''
        def yank_path(self, path):
            """Clear cache of results from a specific path"""
            for func in self._caches:
                cache = {}
                for key in self._caches[func].keys():
                    if path in key[0]:
                        del self._caches[func][key]

    class BundleFuse(Operations):
        """
        A FUSE filesystem implementation for mounting CodaLab bundles

        This is meant to be a read-only filesystem and so all write functionality is omitted.

        If the bundle is a single file, the mountpoint will look like a directory that contains that single file.

        """

        def __init__(self, client, target, verbose=False):
            self.client = client
            self.target = target
            self.bundle_uuid = target[0]
            self.fd = 0 # file descriptor
            self.verbose = verbose
            self.bundle_metadata = self.client.fetch('bundles', self.bundle_uuid)['metadata']

            self.single_file_bundle = False
            info = self._get_info('/')
            if info['type'] == 'file':
                self.single_file_bundle = True
                print 'Note: specified bundle is a single file; mountpoint will look like a directory that contains that single file.'

        # Helpers
        # =======

        @Memoize(timeout=5)
        def _get_info(self, path):
            ''' Set a request through the json api client to get info about the bundle '''
            info = self.client.fetch_contents_info(self.bundle_uuid, path, 1)
            if info is None:
                raise FuseOSError(errno.ENOENT)
            return info

        def verbose_print(self, msg):
            if self.verbose:
                print '[BundleFUSE]:', msg

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

            #TODO: not sure if this is completely correct
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

            byte_range = (offset, offset + length - 1)
            with closing(self.client.fetch_contents_blob(self.bundle_uuid, path, byte_range)) as contents:
                result = contents.read()

            self.verbose_print('read path={}, length={}, offset={}'.format(path, length, offset))
            return result


    def bundle_mount(client, mountpoint, target, verbose=False):
        ''' Mount the filesystem on the mountpoint. '''
        FUSE(BundleFuse(client, target, verbose), mountpoint, nothreads=True, foreground=True)

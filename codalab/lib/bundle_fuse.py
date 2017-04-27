#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import stat

from contextlib import closing

from codalab.lib.fuse import FUSE, FuseOSError, Operations
from codalab.client.json_api_client import JsonApiRelationship
from codalab.lib.path_util import normalize


class BundleFuse(Operations):
    def __init__(self, client, target):
        self.client = client
        self.target = target
        self.bundle_uuid = target[0]
        self.fd = 0

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    def _get_info(self, path):
        info = self.client.fetch_contents_info(self.bundle_uuid, path, 1)
        return info

    # Filesystem methods
    # ==================

    def getattr(self, path, fh=None):
        info = self._get_info(path)

        if info['type'] == 'file':
            mode = stat.S_IFREG | info['perm']
            nlink = 1
        elif info['type'] == 'directory':
            mode = stat.S_IFDIR | info['perm']
            nlink = 2
        elif info['type'] == 'link':
            mode = stat.S_IFLNK | info['perm']
            nlink = 1
        return {
            'st_atime': 0,
            'st_ctime': 0,
            'st_gid': 0,
            'st_mode': mode,
            'st_mtime': 0,
            'st_nlink': nlink,
            'st_uid': 0,
            'st_size': info['size'],
        }


    def readdir(self, path, fh):
        dirents = ['.', '..']
        info = self._get_info(path)
        items = info.get('contents', [])
        for d in items:
            dirents.append(d['name'])
        for r in dirents:
            yield r

    def readlink(self, path):
        info = self._get_info(path)

        pathname = info['link']
        #TODO: not sure if this is completely correct
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname


    # File methods
    # ============

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, length, offset, fh):
        byte_range = (offset, offset + length - 1)
        with closing(self.client.fetch_contents_blob(self.bundle_uuid, path, byte_range)) as contents:
            result = contents.read()
        return result

    '''
    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)
    '''


def bundle_mount(client, mountpoint, target):
    FUSE(BundleFuse(client, target), mountpoint, nothreads=True, foreground=True)

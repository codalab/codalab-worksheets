'''
file_util provides helpers for dealing with file handles in robust,
memory-efficent ways.
'''
BUFFER_SIZE = 2 * 1024 * 1024
#BUFFER_SIZE = 256 * 10240

import sys
import formatting
import urllib2
from codalab.common import UsageError

def copy(source, dest, autoflush=True, print_status=None):
    '''
    Read from the source file handle and write the data to the dest file handle.
    '''
    n = 0
    while True:
        buffer = source.read(BUFFER_SIZE)
        if not buffer:
            break
        dest.write(buffer)
        n += len(buffer)
        if autoflush:
            dest.flush()
        if print_status:
            print >>sys.stderr, "\r%s: %s" % (print_status, formatting.size_str(n)),
            sys.stderr.flush()
    if print_status:
        print >>sys.stderr, "\r%s: %s [done]" % (print_status, formatting.size_str(n))

def download_url(source_url, target_path, print_status=False):
    '''
    Download the file at |source_url| and write it to |target_path|.
    '''
    in_file = urllib2.urlopen(source_url)
    total_bytes = in_file.info().getheader('Content-Length')
    if total_bytes:
        total_bytes = int(total_bytes)

    num_bytes = 0
    out_file = open(target_path, 'wb')
    def status_str():
        if total_bytes:
            return 'Downloaded %s/%s (%d%%)' % (formatting.size_str(num_bytes), formatting.size_str(total_bytes), 100.0 * num_bytes / total_bytes)
        else:
            return 'Downloaded %s' % (formatting.size_str(num_bytes))
    while True:
        s = in_file.read(BUFFER_SIZE)
        if not s: break
        out_file.write(s)
        num_bytes += len(s)
        if print_status:
            print >>sys.stderr, '\r' + status_str(),
            sys.stderr.flush()
    if print_status:
        print >>sys.stderr, '\r' + status_str() + ' [done]'

'''
file_util provides helpers for dealing with file handles in robust,
memory-efficent ways.
'''
BUFFER_SIZE = 2 * 1024 * 1024

import sys
import formatting

def copy(source, dest, autoflush=True, print_status=False):
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
            print "\rCopied %s" % formatting.size_str(n),
            sys.stdout.flush()
    if print_status:
        print "\rCopied %s" % formatting.size_str(n)

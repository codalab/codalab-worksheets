'''
file_util provides helpers for dealing with file handles in robust,
memory-efficent ways.
'''
BUFFER_SIZE = 0x40000


def copy(source, dest, autoflush=True):
    '''
    Read from the source file handle and write the data to the dest file handle.
    '''
    while True:
        buffer = source.read(BUFFER_SIZE)
        if not buffer:
            break
        dest.write(buffer)
        if autoflush:
            dest.flush()

def tail(source, num_lines=10):
    return "\n".join(source.read().splitlines()[-10:])


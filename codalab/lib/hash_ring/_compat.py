import sys

if sys.version_info[0] < 3:
    xrange = xrange
    # sounds weird but I need to set the encoding for python3.3
    bytes = lambda x, y: str(x)
else:
    xrange = range
    bytes = bytes

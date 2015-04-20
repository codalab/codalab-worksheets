import sys

def open_line(s):
    print >>sys.stderr, '\r\033[K%s' % s,
def clear_line():
    print >>sys.stderr, '\r\033[K',

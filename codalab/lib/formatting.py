'''
Provides basic formatting utilities.
'''

import datetime
import sys

def size_str(size):
    '''
    size: number of bytes
    Return a human-readable string.
    '''
    if size == None: return None
    for unit in ('', 'K', 'M', 'G'):
        if size < 100:
            return '%.1f%s' % (size, unit)
        if size < 1024:
            return '%d%s' % (size, unit)
        size /= 1024.0

def date_str(ts):
    return datetime.datetime.fromtimestamp(ts).isoformat().replace('T', ' ')

def duration_str(s):
    '''
    s: number of seconds
    Return a human-readable string.
    Example: 100 => "1m40s", 10000 => "2h46m"
    '''
    if s == None: return None
    m = int(s / 60)
    if m == 0: return "%.1fs" % s
    s -= m * 60

    h = int(m / 60)
    if h == 0: return "%dm%ds" % (m, s)
    m -= h * 60

    d = int(h / 24)
    if d == 0: return "%dh%dm" % (h, m)
    h -= d * 24

    y = int(d / 365)
    if y == 0: return "%dd%dh" % (d, h)
    d -= y * 365

    return "%dy%dd" % (y, d)

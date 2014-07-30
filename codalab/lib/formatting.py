'''
Provides basic formatting utilities.
'''

import datetime

def size_str(size):
    if size == None: return None
    for unit in ('', 'K', 'M', 'G'):
        if size < 1024:
            return '%d%s' % (size, unit)
        size /= 1024

def time_str(ts):
    return datetime.datetime.utcfromtimestamp(ts).isoformat().replace('T', ' ')

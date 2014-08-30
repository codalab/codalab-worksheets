'''
Provides basic formatting utilities.
'''

import datetime
import sys

def size_str(size):
    if size == None: return None
    for unit in ('', 'K', 'M', 'G'):
        if size < 1024:
            return '%d%s' % (size, unit)
        size /= 1024

def date_str(ts):
    return datetime.datetime.fromtimestamp(ts).isoformat().replace('T', ' ')

def duration_str(n):
    units = [60, 60, 24, 365, sys.maxint]
    labels = ['s', 'm', 'h', 'd', 'y']
    for i in range(len(labels)):
        if n < units[i]:
            return '%d%s' % (n, labels[i])
        n /= units[i]

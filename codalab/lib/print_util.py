import sys
import json


def open_line(s, f=sys.stderr):
    print >>f, '\r\033[K%s' % s,


def clear_line(f=sys.stderr):
    print >>f, '\r\033[K',


def pretty_print(obj, f=sys.stdout):
    json.dump(obj, f, sort_keys=True, indent=4, separators=(',', ': '))

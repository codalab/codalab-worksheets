"""
unicode_util provides helpers for working with `unicode` and `str` types
containing unicode characters.
"""


def contains_unicode(s):
    # TODO: use .isascii() for Python 3.7
    return len(s) == len(s.encode())
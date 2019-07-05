"""
unicode_util provides helpers for working with `unicode` and `str` types
containing unicode characters.
"""


def contains_unicode(s):
    # This returns false if everything in s is ASCII, and true otherwise.
    # TODO: use .isascii() for Python 3.7
    return len(s) != len(s.encode())
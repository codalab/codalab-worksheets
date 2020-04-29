"""
unicode_util provides helpers for working with `unicode` and `str` types
containing unicode characters.
"""


def contains_unicode(s):
    try:
        s.encode("ascii")
    except UnicodeError:
        return True
    return False

"""
Provides basic formatting utilities.
"""

import datetime
import sys
import shlex


def contents_str(input_string):
    """
    input_string: raw string (may be None)
    Return a friendly string (if input_string is None, replace with '' for now).
    """
    if input_string is None:
        return ''

    try:
        input_string.decode('utf-8')
    except UnicodeDecodeError:
        return ''

    return input_string


def verbose_contents_str(input_string):
    """
    input_string: raw string (may be None)
    Return a friendly string (which is more verbose than contents_str).
    """
    if input_string is None:
        return '<none>'

    try:
        input_string.decode('utf-8')
    except UnicodeDecodeError:
        return '<binary>'

    return input_string


def size_str(size):
    """
    size: number of bytes
    Return a human-readable string.
    """
    if size is None:
        return None

    for unit in ('', 'K', 'M', 'G'):
        if size < 100 and size != int(size):
            return '%.1f%s' % (size, unit)
        if size < 1024:
            return '%d%s' % (size, unit)
        size /= 1024.0


def date_str(ts):
    return datetime.datetime.fromtimestamp(ts).isoformat().replace('T', ' ')


def duration_str(s):
    """
    s: number of seconds
    Return a human-readable string.
    Example: 100 => "1m40s", 10000 => "2h46m"
    """
    if s is None:
        return None

    m = int(s / 60)
    if m == 0:
        return "%.1fs" % s

    s -= m * 60
    h = int(m / 60)
    if h == 0:
        return "%dm%ds" % (m, s)

    m -= h * 60
    d = int(h / 24)
    if d == 0:
        return "%dh%dm" % (h, m)

    h -= d * 24
    y = int(d / 365)
    if y == 0:
        return "%dd%dh" % (d, h)

    d -= y * 365
    return "%dy%dd" % (y, d)


def parse_size(s):
    """
    s: <number><k|m|g>
    Returns the number of bytes.
    """
    if s[-1].isdigit():
        return float(s)
    n, unit = float(s[0:-1]), s[-1].lower()
    if unit == 'k':
        return n * 1024
    if unit == 'm':
        return n * 1024 * 1024
    if unit == 'g':
        return n * 1024 * 1024 * 1024
    # If error, ignore unit
    print >>sys.stderr, 'Warning: invalid unit in ', s
    raise n


def parse_duration(s):
    """
    s: <number><s|m|h|d|y>
    Returns the number of seconds
    """
    if s[-1].isdigit():
        return float(s)
    n, unit = float(s[0:-1]), s[-1].lower()
    if unit == 's':
        return n
    if unit == 'm':
        return n * 60
    if unit == 'h':
        return n * 60 * 60
    if unit == 'd':
        return n * 60 * 60 * 24
    if unit == 'y':
        return n * 60 * 60 * 24 * 365
    # If error, ignore unit
    print >>sys.stderr, 'Warning: invalid unit in ', s
    raise n

############################################################

# Tokens are serialized as a space-separated list, where we use " to quote.
# "first token" "\"second token\"" third


def quote(token):
    """
    :param token: string token
    :return: properly-quoted string token
    """
    if ' ' in token or '"' in token:
        return '"' + token.replace('"', '\\"') + '"'
    return token


def tokens_to_string(tokens):
    """
    Build string from tokens with proper quoting.

    :param tokens: list of string tokens
    :return: space-separated string of tokens
    """
    return ' '.join(quote(token) for token in tokens)


def string_to_tokens(s):
    """
    Converts string to list of tokens, with support for quotes.
    Defined here for convenience.

    See shlex documentation for more information on parsing rules:
    https://docs.python.org/2/library/shlex.html#parsing-rules

    :param s: string "a b 'c d' e"
    :return: list ["a", "b", "c d", "e"]
    """
    return shlex.split(s, comments=False, posix=True)

"""
Provides basic formatting utilities.
"""

import datetime
import json
import shlex
import pipes


NONE_PLACEHOLDER = '<none>'


def contents_str(input_string, verbose=False):
    """
    :param input_string: any string (may be None)
    :param bool verbose: should use human-readable placeholders instead of empty
                         string to render unprintable string
    Return a printable unicode string.
    """
    if input_string is None:
        return NONE_PLACEHOLDER if verbose else ''
    return str(input_string)


def verbose_contents_str(input_string):
    """
    :param input_string: any string (may be None)
    Return a printable unicode string.
    """
    return contents_str(input_string, verbose=True)


def size_str(size, include_bytes=False):
    """
    size: number of bytes
    include_bytes: whether or not to include 'bytes' string in the return value
    Return a human-readable string.
    """
    if size is None:
        return None

    for unit in ('', 'k', 'm', 'g', 't'):
        if size < 100 and size != int(size):
            if unit == '' and include_bytes:
                return '%.1f bytes' % size
            return '%.1f%s' % (size, unit)

        if size < 1024:
            if unit == '' and include_bytes:
                return '%d bytes' % size
            return '%d%s' % (size, unit)

        size /= 1024.0


def date_str(ts):
    return datetime.datetime.fromtimestamp(ts).isoformat(sep=' ')


def datetime_str(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_datetime(s):
    return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def duration_str(s):
    """
    s: number of seconds
    Return a human-readable string.
    Example: 100 => "1m40s", 10000 => "2h46m"
    """
    if s is None:
        return None

    m = int(s // 60)
    if m == 0:
        return "%.1fs" % s

    s -= m * 60
    h = int(m // 60)
    if h == 0:
        return "%dm%ds" % (m, s)

    m -= h * 60
    d = int(h // 24)
    if d == 0:
        return "%dh%dm" % (h, m)

    h -= d * 24
    y = int(d // 365)
    if y == 0:
        return "%dd%dh" % (d, h)

    d -= y * 365
    return "%dy%dd" % (y, d)


def ratio_str(to_str, a, b):
    """
    Example: to_str = duration_str, a = 60, b = 120 => "1m / 2m (50%)"
    """
    return '%s / %s (%.1f%%)' % (to_str(a), to_str(b), 100.0 * a / b)


def parse_size(s):
    """
    s: <number>[<k|m|g|t>]
    Returns the number of bytes.
    """
    try:
        if s[-1].isdigit():
            return int(s)
        n, unit = float(s[0:-1]), s[-1].lower()
        if unit == 'k':
            return int(n * 1024)
        if unit == 'm':
            return int(n * 1024 * 1024)
        if unit == 'g':
            return int(n * 1024 * 1024 * 1024)
        if unit == 't':
            return int(n * 1024 * 1024 * 1024 * 1024)
    except (IndexError, ValueError):
        pass  # continue to next line and throw error
    raise ValueError('Invalid size: %s, expected <number>[<k|m|g|t>]' % s)


def parse_duration(s):
    """
    s: <number>[<s|m|h|d|y>]
    Returns the number of seconds
    """
    try:
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
    except (IndexError, ValueError):
        pass  # continue to next line and throw error
    raise ValueError('Invalid duration: %s, expected <number>[<s|m|h|d|y>]' % s)


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
    return ' '.join(map(pipes.quote, tokens))


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


def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))


def verbose_pretty_json(obj):
    if obj is None:
        return NONE_PLACEHOLDER
    return pretty_json(obj)


def key_value_list(pairs):
    return "\n".join([("%s=%r" % tuple(p)) for p in pairs])

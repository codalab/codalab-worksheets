"""
Provides basic formatting utilities.
"""

import datetime
import json
import shlex
import pipes

from codalabworker import formatting as worker_formatting


NONE_PLACEHOLDER = '<none>'
BINARY_PLACEHOLDER = '<binary>'


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
        return NONE_PLACEHOLDER

    try:
        input_string.decode('utf-8')
    except UnicodeDecodeError:
        return BINARY_PLACEHOLDER

    return input_string


size_str = worker_formatting.size_str


def date_str(ts):
    return datetime.datetime.fromtimestamp(ts).isoformat(sep=' ')


def datetime_str(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_datetime(s):
    return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


duration_str = worker_formatting.duration_str


def ratio_str(to_str, a, b):
    """
    Example: to_str = duration_str, a = 60, b = 120 => "1m / 2m (50%)"
    """
    return '%s / %s (%.1f%%)' % (to_str(a), to_str(b), 100.0 * a / b)


parse_size = worker_formatting.parse_size


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

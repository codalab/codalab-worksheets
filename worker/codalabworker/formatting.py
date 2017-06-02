def size_str(size):
    """
    size: number of bytes
    Return a human-readable string.
    """
    if size is None:
        return None

    for unit in ('', 'k', 'm', 'g', 't'):
        if size < 100 and size != int(size):
            return '%.1f%s' % (size, unit)
        if size < 1024:
            return '%d%s' % (size, unit)
        size /= 1024.0


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
    s: <number>[<k|m|g|t>]
    Returns the number of bytes.
    """
    try:
        if s[-1].isdigit():
            return float(s)
        n, unit = float(s[0:-1]), s[-1].lower()
        if unit == 'k':
            return n * 1024
        if unit == 'm':
            return n * 1024 * 1024
        if unit == 'g':
            return n * 1024 * 1024 * 1024
        if unit == 't':
            return n * 1024 * 1024 * 1024 * 1024
    except (IndexError, ValueError):
        pass  # continue to next line and throw error
    raise ValueError('Invalid size: %s, expected <number>[<k|m|g|t>]' % s)

import re

from codalab.common import precondition, UsageError


def nested_dict_get(obj, *args, **kwargs):
    """
    Get a value from a nested dictionary.

    Cleans up calls that look lke this:
        bundle_info.get('owner', {}).get('user_name', None)

    And turns them into:
        safe_get(bundle_info, 'owner', 'user_name')

    :param o: dict-like object to 'get' value from
    :param args: variable list of nested keys
    :param kwargs: supports the kwarg 'default' to specify the default value to
                   return if any of the keys don't exist. (default is None)
                   Any other kwarg will raise an exception.
    :return: retrieved value or default if it doesn't exist
    """
    default = kwargs.pop('default', None)
    precondition(not kwargs, 'unsupported kwargs %s' % kwargs.keys())
    try:
        for arg in args:
            obj = obj[arg]
        return obj
    except (KeyError, TypeError):
        return default

def parse_target_spec(spec):
    key = ''
    bundle_spec = ''
    if '::' in spec:
        prefix, suffix = spec.split('::')
    else:
        prefix, suffix = spec, None
    if ':' in prefix:  # :<bundle_spec> or <key>:<bundle_spec>
        key, bundle_prefix = prefix.split(':', 1)
        bundle_spec = bundle_prefix if suffix is None else bundle_prefix + "::" + suffix
        if key == '':
            key = bundle_spec
    else:  # <bundle_spec>
        bundle_spec = spec
    return key, bundle_spec

def desugar_command(orig_target_spec, command):
    """
    Desugar command, returning mutated target_spec and command.
    Examples:
    - %a.txt% => [b1:a.txt], b1
    - %:a.txt% => [:a.txt], a.txt (implicit key is a.txt)
    - %instance::ws//a.txt% => [b1:instance::ws//a.txt], b1
    - %corenlp%/run %a.txt% => [b1:corenlp, b2:a.txt], b1/run b2
    - %:word-vectors//glove.6B%/vector.txt => [:word-vectors//glove.6B], :word-vectors//glove.6B/vector.txt
    """
    # If key is not specified, use b1, b2, b3 by default.
    pattern = re.compile('^([^%]*)%([^%]+)%(.*)$')
    buf = ''  # Build up the modified command

    key2val = {}  # e.g., b1 => a.txt
    val2key = {}  # e.g., a.txt => b1 (use first key)

    def get(dep):  # Return the key
        key, val = parse_target_spec(dep)
        if key == '':
            val = dep
            if val in val2key:
                key = val2key[val]
            else:
                key = 'b' + str(len(target_spec) + 1)  # new key

        if val not in val2key:
            val2key[val] = key
        if key in key2val:
            if key2val[key] != val:
                raise UsageError('key %s exists with multiple values: %s and %s' % (key, key2val[key], val))
        else:
            key2val[key] = val
            target_spec.append((key if key != val else '') + ':' + val)
        return key

    target_spec = []
    for spec in orig_target_spec:
        get(spec)

    while True:
        match = pattern.match(command)
        if not match:
            break

        buf += match.group(1) + get(match.group(2))
        command = match.group(3)

    return (target_spec, buf + command)

import re
import datetime

from codalab.common import precondition, UsageError

INSTANCE_SEPARATOR = "::"
WORKSHEET_SEPARATOR = "//"

TARGET_KEY_REGEX = r"(?<=^)(?:([^:]*?)\:(?!:))?(.*(?=$))"
TARGET_REGEX = r"(?<=^)(?:(.*?)\:\:)?(?:(.*?)\/\/)?(.+?)(?:\/(.*?))?(?=$)"


def nested_dict_get(obj, *args, **kwargs):
    """
    Get a value from a nested dictionary.

    Cleans up calls that look lke this:
        bundle_info.get('owner', {}).get('user_name', None)

    And turns them into:
        safe_get(bundle_info, 'owner', 'user_name')

    :param obj: dict-like object to 'get' value from
    :param args: variable list of nested keys
    :param kwargs: supports the kwarg 'default' to specify the default value to
                   return if any of the keys don't exist. (default is None)
                   Any other kwarg will raise an exception.
    :return: retrieved value or default if it doesn't exist
    """
    default = kwargs.pop('default', None)
    precondition(not kwargs, 'unsupported kwargs %s' % list(kwargs.keys()))
    try:
        for arg in args:
            obj = obj[arg]
        return obj
    except (KeyError, TypeError):
        return default


def parse_key_target(spec):
    """
    Parses a keyed target spec into its key and the rest of the target spec
    :param spec: a target spec in the form of
        [[<key>]:][<instance>::][<worksheet_spec>//]<bundle_spec>[/<subpath>]
    where <bundle_spec> is required and the rest are optional.

    :return: a tuple of the following in that order:
        - <key>: (<key> if present,
                    empty string if ':' in spec but no <key>,
                    None otherwise)
        - <value> (where value is everyhing after a <key>: (or everything if no key specified)
    """

    match = re.match(TARGET_KEY_REGEX, spec)
    return match.groups()


def parse_target_spec(spec):
    """
    Parses a (non-keyed) target spec into its components
        :param spec: a target spec in the form of
            [<instance>::][<worksheet_spec>//]<bundle_spec>[/<subpath>]
    where <bundle_spec> is required and the rest are optional.

    :return: a tuple of the following in that order:
        - <instance>
        - <worksheet_spec>
        - <bundle_spec>
        - <subpath>
    """

    match = re.match(TARGET_REGEX, spec)
    return match.groups() if match else (None, None, None, None)


def desugar_command(orig_target_spec, command):
    """
    Desugar command, returning mutated target_spec and command.
    Examples:
    - %a.txt% => [b1:a.txt], b1
    - %:a.txt% => [:a.txt], a.txt (implicit key is a.txt)
    - %instance::ws//a.txt% => [b1:instance::ws//a.txt], b1
    - %corenlp%/run %a.txt% => [b1:corenlp, b2:a.txt], b1/run b2
    - %:word-vectors//glove.6B%/vector.txt =>
        [glove.6B/vector.txt:word-vectors//glove.6B/vector.txt], glove.6B/vector.txt
    """
    # If key is not specified, use b1, b2, b3 by default.
    pattern = re.compile('^([^%]*)%([^%]+)%(.*)$')
    buf = ''  # Build up the modified command

    key2val = {}  # e.g., b1 => a.txt
    val2key = {}  # e.g., a.txt => b1 (use first key)

    def get(dep):  # Return the key
        key, val = parse_key_target(dep)
        if key == '':
            # key only matches empty string if ':' present
            _, _, bundle, subpath = parse_target_spec(val)
            key = subpath if subpath is not None else bundle
        elif key is None:
            # key only returns None if ':' not present in original spec
            key = val2key[val] if val in val2key else 'b' + str(len(target_spec) + 1)

        if val not in val2key:
            val2key[val] = key
        if key in key2val:
            if key2val[key] != val:
                raise UsageError(
                    'key %s exists with multiple values: %s and %s' % (key, key2val[key], val)
                )
        else:
            key2val[key] = val
            target_spec.append(key + ':' + val)
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

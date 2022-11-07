import re
import os

from codalab.common import precondition, UsageError

INSTANCE_SEPARATOR = "::"
WORKSHEET_SEPARATOR = "//"

TARGET_KEY_REGEX = r"(?<=^)(?:([^:]*?)\:(?!:))?(.*(?=$))"
TARGET_REGEX = r"(?<=^)(?:(.*?)\:\:)?(?:(.*?)\/\/)?(.+?)(?:\/(.*?))?(?=$)"

# Formatting Constants
ADDRESS_SPEC_FORMAT = "(<alias>|<address>)"
BASIC_SPEC_FORMAT = '(<uuid>|<name>)'
BASIC_BUNDLE_SPEC_FORMAT = '(<uuid>|<name>|^<index>)'

GLOBAL_SPEC_FORMAT = "[%s%s]%s" % (ADDRESS_SPEC_FORMAT, INSTANCE_SEPARATOR, BASIC_SPEC_FORMAT)
WORKSHEET_SPEC_FORMAT = GLOBAL_SPEC_FORMAT

BUNDLE_SPEC_FORMAT = '[%s%s]%s' % (
    WORKSHEET_SPEC_FORMAT,
    WORKSHEET_SEPARATOR,
    BASIC_BUNDLE_SPEC_FORMAT,
)

BUNDLES_URL_SEPARATOR = '/bundles/'
WORKSHEETS_URL_SEPARATOR = '/worksheets/'

TARGET_SPEC_FORMAT = '%s[%s<subpath within bundle>]' % (BUNDLE_SPEC_FORMAT, os.sep)
RUN_TARGET_SPEC_FORMAT = '[<key>]:' + TARGET_SPEC_FORMAT
MAKE_TARGET_SPEC_FORMAT = '[<key>:]' + TARGET_SPEC_FORMAT
GROUP_SPEC_FORMAT = '(<uuid>|<name>|public)'
PERMISSION_SPEC_FORMAT = '((n)one|(r)ead|(a)ll)'
UUID_POST_FUNC = '[0:8]'  # Only keep first 8 characters


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
    Parses a keyed target spec into its key and the rest of the target spec.
    Raise UsageError when the value of the spec is empty.
    :param spec: a target spec in the form of
        [[<key>]:][<instance>::][<worksheet_spec>//]<bundle_spec>[/<subpath>]
    where <bundle_spec> is required and the rest are optional.

    :return: a tuple of the following in that order:
        - <key>: (<key> if present,
                    empty string if ':' in spec but no <key>,
                    None otherwise)
        - <value> (where value is everything after a <key>: (or everything if no key specified)
    """
    match = re.match(TARGET_KEY_REGEX, spec)
    key, value = match.groups()
    # This check covers three usage errors:
    # 1. both key and value are empty, e.g. "cl run : 'echo a'"
    # 2. key is not empty, value is empty, e.g. "cl run a.txt: 'echo a'"
    if value == '':
        raise UsageError(
            'target_spec (%s) in wrong format. Please provide a valid target_spec in the format of %s.'
            % (spec, RUN_TARGET_SPEC_FORMAT)
        )
    return (key, value)


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

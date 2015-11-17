import re

from codalab.common import UsageError


def desugar_command(orig_target_spec, command):
    """
    Desugar command, returning mutated target_spec and command.
    Examples:
    - %a.txt% => [b1:a.txt], b1
    - %:a.txt% => [:a.txt], a.txt (implicit key is a.txt)
    - %corenlp%/run %a.txt% => [b1:corenlp, b2:a.txt], b1/run b2
    """
    # If key is not specified, use b1, b2, b3 by default.
    pattern = re.compile('^([^%]*)%([^%]+)%(.*)$')
    buf = ''  # Build up the modified command

    key2val = {}  # e.g., b1 => a.txt
    val2key = {}  # e.g., a.txt => b1 (use first key)

    def get(dep):  # Return the key
        if ':' in dep:  # :<bundle_spec> or <key>:<bundle_spec>
            key, val = dep.split(':', 1)
            if key == '':
                key = val
        else:  # <bundle_spec>
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
    for x in orig_target_spec:
        get(x)

    while True:
        m = pattern.match(command)
        if not m:
            break

        buf += m.group(1) + get(m.group(2))
        command = m.group(3)

    return (target_spec, buf + command)

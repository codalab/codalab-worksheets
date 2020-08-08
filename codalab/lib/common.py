import json
import os
import sys

from codalab.lib import path_util
from codalab.lib.print_util import pretty_print_json


def get_codalab_home():
    # Default to this directory in the user's home directory.
    # In the future, allow customization based on.
    home = os.getenv('CODALAB_HOME', '~/.codalab')
    home = path_util.normalize(home)
    path_util.make_directory(home)
    return home


def read_json_or_die(path):
    with open(path, 'r') as f:
        string = f.read()
    try:
        return json.loads(string)
    except ValueError as e:
        print("Invalid JSON in %s:\n%s" % (path, string))
        print(e)
        sys.exit(1)


def write_pretty_json(data, path):
    with open(path, 'w') as f:
        pretty_print_json(data, f)

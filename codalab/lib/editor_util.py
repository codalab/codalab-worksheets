
import os
import subprocess
import sys
import tempfile

from codalab.lib import path_util
from codalab.lib.codalab_manager import CodaLabManager

def find_default_editor():
    manager = CodaLabManager()
    editor = os.getenv('EDITOR')
    if editor:
        return editor
    # If not yet set, use a sane default.
    if sys.platform == 'win32':
        editor = 'notepad'
    else:
        editor = 'vi'
    return editor

def open_and_edit(suffix, template=''):
    editor = find_default_editor()
    tempfile_name = ''
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as form:
        form.write(template.encode('utf-8'))
        form.flush()
        tempfile_name = form.name
    lines = ''
    if os.path.isfile(tempfile_name):
        subprocess.call([editor, tempfile_name])
        with open(tempfile_name, 'r') as form:
            lines = form.readlines()
            lines = [line.decode('utf-8') for line in lines]
        path_util.remove(tempfile_name)

    return lines



import os
import subprocess
import sys
import tempfile

from codalab.lib import path_util
from codalab.lib.codalab_manager import CodaLabManager


def is_installed(name):
    try:
        devnull = open(os.devnull)
        subprocess.Popen([name], stdout=devnull, stderr=devnull)
    except OSError as e:
        if e.errno == os.errno.ENOENT:
            return False
    return True


def find_default_editor():
    manager = CodaLabManager()
    editor = manager.config.get('editor', None)
    if editor:
        return editor
    #not yet set, lets set a sane default.
    if sys.platform == 'win32':
        editor = 'notepad'
    else:
        if is_installed('vim'):
            editor = 'vim'
        else:
            editor = 'vi'
    manager.config['editor'] = editor
    manager.save_config()
    return editor

def open_and_edit(suffix, template=''):
    editor = find_default_editor()
    tempfile_name = ''
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as form:
        form.write(template)
        form.flush()
        tempfile_name = form.name
    lines = ''
    if os.path.isfile(tempfile_name):
        subprocess.call([editor, tempfile_name])
        with open(tempfile_name, 'rb') as form:
            lines = form.readlines()
        path_util.remove(tempfile_name)

    return lines


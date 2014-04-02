'''
worksheet_util contains the following public functions:
- get_current_items: get the current items of a worksheet.
- request_new_items: pops up an editor to allow for full-text editing of a worksheet.
'''
import os
import re
import subprocess
import sys
import tempfile

from codalab.common import UsageError


BUNDLE_LINE_REGEX = re.compile('^(\[(.*)\])?\s*\{(.*)\}$')


def get_worksheet_lines(worksheet_info):
    '''
    Generator that returns pretty-printed lines of text for the given worksheet.
    '''
    for (bundle_info, value) in worksheet_info['items']:
        if bundle_info is None:
            yield value
        else:
            if 'bundle_type' not in bundle_info:
                yield '// The following bundle reference is broken:'
            yield '[%s]{%s}' % (value, bundle_info['uuid'])


def get_current_items(worksheet_info):
    '''
    Return list of (bundle_uuid, value) pairs.
    Note: worksheet_info['items'] contains (bundle_info, value)
    '''
    items = []
    for (bundle_info, value) in worksheet_info['items']:
        if bundle_info is None:
            items.append((None, value))
        else:
            items.append((bundle_info['uuid'], value))
    return items


def request_new_items(worksheet_info):
    '''
    Input: a worksheet info dict.
    Popup an editor, populated with the current worksheet contents.
    Return a list of new items that the user typed into the editor.
    '''
    header = '''
// Full-text editing for worksheet %s. This file is basically Markdown, except
// that is used for comments and that lines of the form {bundle_spec} are
// resolved as links to CodaLab bundles. You can even preface the curly braces
// with bracketed help text, like so: [some explanatory text]{bundle_spec}
    '''.strip() % (worksheet_info['name'],)
    # Construct a form template with the current value of the worksheet.
    template_lines = header.split('\n')
    template_lines.extend(get_worksheet_lines(worksheet_info))
    template = os.linesep.join(template_lines)
    # Show the form to the user in their editor of choice and parse the result.
    editor = os.environ.get('EDITOR', 'notepad' if sys.platform == 'win32' else 'vim')
    tempfile_name = ''
    with tempfile.NamedTemporaryFile(suffix='.c', delete=False) as form:
        form.write(template)
        form.flush()
        tempfile_name = form.name
    lines = template_lines
    if os.path.isfile(tempfile_name):
        subprocess.call([editor, tempfile_name])
        with open(tempfile_name, 'rb') as form:
            lines = form.readlines()
        os.remove(tempfile_name)
    form_result = [line[:-1] if line.endswith('\n') else line for line in lines]
    if form_result == template_lines:
        raise UsageError('No change made; aborting')
    return parse_worksheet_form(form_result)


def parse_worksheet_form(form_result):
    '''
    Parse the result of a form template produced in request_missing_metadata.
    Return a list of (bundle_uuid, value) pairs, where bundle_uuid could be None.
    '''
    result = []
    for line in form_result:
        line = line.strip()
        if line[:2] == '//':
            continue
        match = BUNDLE_LINE_REGEX.match(line)
        if match:
            # Parse a (bundle_uuid, value) pair out of the bundle line.
            # Note that the value could be None (if there was no [])
            value = match.group(2)
            value = value if value is None else value.strip()
            result.append((match.group(3).strip(), value))
        else:
            result.append((None, line))
    return result

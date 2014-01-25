'''
worksheet_util contains one public function, request_new_items, which pops
up an editor to allow for full-text editing of a worksheet.
'''
import os
import re
import subprocess
import tempfile

from codalab.common import UsageError


BUNDLE_LINE_REGEX = re.compile('^(\[(.*)\])?\s*\{(.*)\}$')


def request_new_items(worksheet_info):
  '''
  Take a worksheet info dict. Return a list of new items provided in an editor.
  '''
  header = '''
// Full-text editing for worksheet %s. This file is basically Markdown, except
// that // is used for comments and that lines of the form {bundle_spec} are
// resolved as links to CodaLab bundles. You can even preface the curly braces
// with bracketed help text, like so: [some explanatory text]{bundle_spec}
  '''.strip() % (worksheet_info['name'],)
  # Construct a form template with the current value of the worksheet.
  template_lines = header.split('\n')
  for (bundle_info, value) in worksheet_info['items']:
    if bundle_info is None:
      template_lines.append(value)
    else:
      template_lines.append('[%s]{%s}' % (value, bundle_info['uuid']))
  template = '\n'.join(template_lines)
  # Show the form to the user in their editor of choice and parse the result.
  editor = os.environ.get('EDITOR', 'vim')
  with tempfile.NamedTemporaryFile(suffix='.c') as form:
    form.write(template)
    form.flush()
    subprocess.call([editor, form.name])
    with open(form.name, 'rb') as form:
      lines = form.readlines()
  form_result = [line[:-1] if line.endswith('\n') else line for line in lines]
  if form_result == template_lines:
    raise UsageError('No change made; aborting.')
  return parse_worksheet_form(form_result)


def parse_worksheet_form(form_result):
  '''
  Parse the result of a form template produced in request_missing_metadata.
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

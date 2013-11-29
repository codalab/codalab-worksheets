import os
import platform
import subprocess
import tempfile


machine = platform.machine()
metadata_defaults = {
  'architecture': [machine] if machine else [],
}

metadata_key_to_argument = lambda metadata_key: 'md_%s' % (metadata_key,)


def add_metadata_arguments(bundle_subclass, metadata_keys, parser):
  '''
  Add arguments to a command-line argument parser for all metadata keys
  needed by the given bundle subclass. Skip keys already in metadata_keys.
  '''
  help_suffix = ''
  if bundle_subclass.BUNDLE_TYPE:
    help_suffix = ' (for %ss)' % (bundle_subclass.BUNDLE_TYPE,)
  for (metadata_key, short_key, help_text) in bundle_subclass.METADATA_SPEC:
    if metadata_key not in metadata_keys:
      metadata_keys.add(metadata_key)
      metadata_type = bundle_subclass.METADATA_TYPES[metadata_key]
      parser.add_argument(
        '--%s' % (short_key,),
        default=metadata_defaults.get(metadata_key),
        dest=metadata_key_to_argument(metadata_key,),
        help=(help_text + help_suffix),
        metavar=short_key.upper(),
        nargs=('*' if metadata_type == set else None),
      )


def request_missing_metadata(bundle_subclass, args):
  '''
  For any metadata arguments that were not supplied through the command line,
  pop up an editor and request that data from the user.
  '''
  metadata_types = bundle_subclass.METADATA_TYPES
  initial_metadata = {
    metadata_key: getattr(args, metadata_key_to_argument(metadata_key,))
    for metadata_key in metadata_types
  }
  # A special-case: if the user specified all required metadata on the command
  # line, do NOT show the editor. This allows for programmatic bundle creation.
  if not any(value is None for value in initial_metadata.values()):
    return initial_metadata
  # Construct a form template with the required keys, prefilled with the
  # command-line metadata options.
  template_lines = []
  for (metadata_key, short_key, help_text) in bundle_subclass.METADATA_SPEC:
    metadata_type = bundle_subclass.METADATA_TYPES[metadata_key]
    initial_value = initial_metadata.get(metadata_key) or ''
    if metadata_type == set:
      initial_value = ' '.join(initial_value or [])
    template_lines.append('%s: %s' % (metadata_key.title(), initial_value))
  bundle_type = bundle_subclass.BUNDLE_TYPE
  template_lines.append('\n'.join([
    '# Record metadata for the new %s, then save and quit.' % (bundle_type,),
    '# Leave the name blank to cancel the upload.',
  ]))
  template = '\n\n'.join(template_lines)
  # Show the form to the user in their editor of choice and parse the result.
  editor = os.environ.get('EDITOR', 'vim')
  with tempfile.NamedTemporaryFile(suffix='.sh') as form:
    form.write(template)
    form.flush()
    subprocess.call([editor, form.name])
    return parse_metadata_form(bundle_subclass, form.name)


def parse_metadata_form(bundle_subclass, form_name):
  '''
  Parse the result of a form template produced in request_missing_metadata.
  '''
  result = {}
  with open(form_name) as form:
    for line in form:
      line = line.strip()
      if line and not line.startswith('#'):
        if ':' not in line:
          raise ValueError('Malformatted line (no colon): %s' % (line,))
        (metadata_key, remainder) = line.split(':', 1)
        metadata_key = metadata_key.lower()
        if metadata_key not in bundle_subclass.METADATA_TYPES:
          raise ValueError('Unexpected metadata key: %s' % (metadata_key,))
        metadata_type = bundle_subclass.METADATA_TYPES[metadata_key]
        if metadata_type == set:
          result[metadata_key] = [
            subpart for part in remainder.strip().split()
            for subpart in part.split(',') if subpart
          ]
        else:
          result[metadata_key] = remainder.strip()
  return result

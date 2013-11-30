import os
import subprocess
import tempfile


metadata_key_to_argument = lambda metadata_key: 'md_%s' % (metadata_key,)


def add_metadata_arguments(bundle_subclass, metadata_keys, parser):
  '''
  Add arguments to a command-line argument parser for all metadata keys
  needed by the given bundle subclass. Skip keys already in metadata_keys.
  '''
  help_suffix = ''
  if bundle_subclass.BUNDLE_TYPE:
    help_suffix = ' (for %ss)' % (bundle_subclass.BUNDLE_TYPE,)
  for spec in bundle_subclass.METADATA_SPECS:
    if spec.key not in metadata_keys:
      metadata_keys.add(spec.key)
      parser.add_argument(
        '--%s' % (spec.short_key,),
        default=spec.default,
        dest=metadata_key_to_argument(spec.key,),
        help=(spec.description + help_suffix),
        metavar=spec.short_key.upper(),
        nargs=('*' if spec.type == set else None),
      )


def request_missing_metadata(bundle_subclass, args):
  '''
  For any metadata arguments that were not supplied through the command line,
  pop up an editor and request that data from the user.
  '''
  initial_metadata = {
    spec.key: getattr(args, metadata_key_to_argument(spec.key,))
    for spec in bundle_subclass.METADATA_SPECS
  }
  # A special-case: if the user specified all required metadata on the command
  # line, do NOT show the editor. This allows for programmatic bundle creation.
  if not any(value is None for value in initial_metadata.values()):
    return initial_metadata
  # Construct a form template with the required keys, prefilled with the
  # command-line metadata options.
  template_lines = []
  for spec in bundle_subclass.METADATA_SPECS:
    initial_value = initial_metadata.get(spec.key) or ''
    if spec.type == set:
      initial_value = ' '.join(initial_value or [])
    template_lines.append('%s: %s' % (spec.key.title(), initial_value))
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
    return parse_metadata_form(bundle_subclass.METADATA_SPECS, form.name)


def parse_metadata_form(metadata_specs, form_name):
  '''
  Parse the result of a form template produced in request_missing_metadata.
  '''
  metadata_types = {spec.key: spec.type for spec in metadata_specs}
  result = {}
  with open(form_name) as form:
    for line in form:
      line = line.strip()
      if line and not line.startswith('#'):
        if ':' not in line:
          raise ValueError('Malformatted line (no colon): %s' % (line,))
        (metadata_key, remainder) = line.split(':', 1)
        metadata_key = metadata_key.lower()
        if metadata_key not in metadata_types:
          raise ValueError('Unexpected metadata key: %s' % (metadata_key,))
        metadata_type = metadata_types[metadata_key]
        if metadata_type == set:
          result[metadata_key] = [
            subpart for part in remainder.strip().split()
            for subpart in part.split(',') if subpart
          ]
        else:
          result[metadata_key] = remainder.strip()
  return result

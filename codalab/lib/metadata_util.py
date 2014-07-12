'''
metadata_util contains methods for dealing with command-line metadata input.

The add_arguments function takes an ArgumentParser and adds arguments for
the metadata for a given bundle type. After parsing these arguments, all
of which are optional on the CLI but some of which are required for the
bundle itself, call request_missing_metadata to pop up an editor to get the
missing metadata values.
'''
import os
import subprocess
import sys
import tempfile

from codalab.common import UsageError
from codalab.lib.metadata_defaults import MetadataDefaults
from codalab.lib import path_util


metadata_key_to_argument = lambda metadata_key: 'md_%s' % (metadata_key,)


def add_arguments(bundle_subclass, metadata_keys, parser):
    '''
    Add arguments to a command-line argument parser for all metadata keys
    needed by the given bundle subclass. Skip keys already in metadata_keys.
    '''
    help_suffix = ''
    if bundle_subclass.BUNDLE_TYPE:
        help_suffix = ' (for %ss)' % (bundle_subclass.BUNDLE_TYPE,)
    for spec in bundle_subclass.get_user_defined_metadata():
        if spec.key not in metadata_keys:
            metadata_keys.add(spec.key)
            parser.add_argument(
              '--%s' % (spec.short_key,),
              dest=metadata_key_to_argument(spec.key,),
              help=(spec.description + help_suffix),
              metavar=spec.metavar,
              nargs=('*' if spec.type == set else None),
            )


def add_auto_argument(parser):
    '''
    Adds a --auto argument that will skip showing the editor to request any
    unspecified metadata values.
    '''
    parser.add_argument(
      '-a', '--auto',
      action='store_true',
      help="use metadata defaults and don't show an editor",
    )


def request_missing_data(bundle_subclass, args, initial_metadata=None):
    '''
    For any metadata arguments that were not supplied through the command line,
    pop up an editor and request that data from the user.
    '''
    if not initial_metadata:
        initial_metadata = {
          spec.key: getattr(args, metadata_key_to_argument(spec.key,))
          for spec in bundle_subclass.get_user_defined_metadata()
        }
        # A special-case: if the user specified all required metadata on the command
        # line, do NOT show the editor. This allows for programmatic bundle creation.
        if not any(value is None for value in initial_metadata.values()):
            return initial_metadata
    # Fill in default values for all unsupplied metadata keys.
    for spec in bundle_subclass.get_user_defined_metadata():
        if not initial_metadata[spec.key]:
            default = MetadataDefaults.get_default(spec, bundle_subclass, args)
            initial_metadata[spec.key] = default
    # If the --auto flag was used, skip showing the editor.
    if getattr(args, 'auto', False):
        return filter_anonymous_name(bundle_subclass, initial_metadata)
    # Construct a form template with the required keys, prefilled with the
    # command-line metadata options.
    template_lines = []
    bundle_type = bundle_subclass.BUNDLE_TYPE
    template_lines.append(os.linesep.join([
      '// Enter metadata for the new %s bundle, then save and quit.' % (bundle_type,),
      '// To cancel the upload, delete the name.',
    ]))
    for spec in bundle_subclass.get_user_defined_metadata():
        initial_value = initial_metadata.get(spec.key) or ''
        if spec.type == set:
            initial_value = ' '.join(initial_value or [])
        template_lines.append('%s: %s' % (spec.key, initial_value))
    template = (os.linesep + os.linesep).join(template_lines)
    # Show the form to the user in their editor of choice and parse the result.
    editor = os.environ.get('EDITOR', 'notepad' if sys.platform == 'win32' else 'vim')
    tempfile_name = ''
    with tempfile.NamedTemporaryFile(suffix='.sh', delete=False) as form:
        form.write(template)
        form.flush()
        tempfile_name = form.name
    if os.path.isfile(tempfile_name):
        subprocess.call([editor, tempfile_name])
        with open(tempfile_name, 'rb') as form:
            form_result = form.readlines()
        path_util.remove(tempfile_name)
    return parse_metadata_form(bundle_subclass, form_result)

def parse_metadata_form(bundle_subclass, form_result):
    '''
    Parse the result of a form template produced in request_missing_metadata.
    '''
    metadata_specs = bundle_subclass.get_user_defined_metadata()
    metadata_types = {spec.key: spec.type for spec in metadata_specs}
    result = {}
    for line in form_result:
        line = line.strip()
        if line and not line.startswith('//'):
            if ':' not in line:
                # TODO: don't delete everything; go back to the editor
                raise UsageError('Malformatted line (no colon): %s' % (line,))
            (metadata_key, remainder) = line.split(':', 1)
            # TODO: handle multiple lines
            if metadata_key not in metadata_types:
                raise UsageError('Unexpected metadata key: %s' % (metadata_key,))
            metadata_type = metadata_types[metadata_key]
            if metadata_type == set:
                result[metadata_key] = remainder.replace(',', ' ').strip().split()
            else:
                result[metadata_key] = remainder.strip()
    if 'name' not in result:
        raise UsageError('No name specified; aborting')
    return filter_anonymous_name(bundle_subclass, result)


def filter_anonymous_name(bundle_subclass, metadata):
    '''
    If the user left an anonymous name for this bundle, wipe it out and let the
    bundle subclass's constructor choose a name instead.
    '''
    anonymous_name = MetadataDefaults.get_anonymous_name(bundle_subclass)
    if metadata.get('name') == anonymous_name:
        metadata['name'] = None
    return metadata

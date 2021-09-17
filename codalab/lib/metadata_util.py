'''
metadata_util contains methods for dealing with command-line metadata input.

The add_arguments function takes an ArgumentParser and adds arguments for
the metadata for a given bundle type. After parsing these arguments, all
of which are optional on the CLI but some of which are required for the
bundle itself, call request_missing_metadata to pop up an editor to get the
missing metadata values.
'''
import os

from codalab.common import UsageError
from codalab.lib.metadata_defaults import MetadataDefaults
from codalab.lib import editor_util, unicode_util

metadata_key_to_argument = lambda metadata_key: 'md_%s' % (metadata_key,)
metadata_argument_to_key = lambda arg_key: arg_key[3:]


def fill_missing_metadata(bundle_subclass, args, initial_metadata):
    '''
    Return metadata for bundles by filling in the missing metadata with default values.
    args: Namespace object created from attributes parsed out of the command line. See
        `argparse` for more information
    '''
    # Fill in default values for all unsupplied metadata keys.
    new_initial_metadata = {}
    for spec in bundle_subclass.get_user_defined_metadata():
        new_initial_metadata[spec.key] = initial_metadata.get(spec.key)
        if new_initial_metadata[spec.key] is None:
            default = MetadataDefaults.get_default(spec, bundle_subclass, args)
            new_initial_metadata[spec.key] = default
        final_value = new_initial_metadata[spec.key]
        is_unicode_string = isinstance(final_value, str) and unicode_util.contains_unicode(
            final_value
        )
        is_unicode_list = isinstance(final_value, list) and any(
            unicode_util.contains_unicode(v) for v in final_value
        )
        if is_unicode_string or is_unicode_list:
            raise UsageError('Metadata cannot contain unicode: %s = %s' % (spec.key, final_value))

    return new_initial_metadata


def request_missing_metadata(bundle_subclass, initial_metadata):
    '''
    Pop up an editor and request data from the user.
    '''
    # Construct a form template with the required keys, prefilled with the
    # command-line metadata options.
    template_lines = []
    bundle_type = bundle_subclass.BUNDLE_TYPE
    template_lines.append(
        os.linesep.join(
            [
                '// Enter metadata for the new %s bundle, then save and quit.' % (bundle_type,),
                '// To cancel the upload, delete the name.',
            ]
        )
    )
    for spec in bundle_subclass.get_user_defined_metadata():
        initial_value = initial_metadata.get(spec.key) or ''
        if spec.type == list:
            initial_value = ' '.join(initial_value or [])
        template_lines.append('')
        template_lines.append('// %s' % spec.description)
        template_lines.append('%s: %s' % (spec.key, initial_value))
    template = os.linesep.join(template_lines)

    # Show the form to the user in their editor of choice and parse the result.
    form_result = editor_util.open_and_edit(suffix='.c', template=template)
    return parse_metadata_form(bundle_subclass, form_result)


def parse_metadata_form(bundle_subclass, form_result):
    '''
    Parse the result of a form template produced out request_missing_metadata.
    '''
    metadata_specs = bundle_subclass.get_user_defined_metadata()
    metadata_types = {spec.key: spec.type for spec in metadata_specs}
    result = {}
    for line in form_result:
        line = line.strip()
        if line != '' and not line.startswith('//'):
            if ':' not in line:
                # TODO: don't delete everything; go back to the editor and show the error message
                raise UsageError('Malformatted line (no colon): %s' % (line,))
            (metadata_key, remainder) = line.split(':', 1)
            remainder = remainder.strip()
            if remainder == '':
                remainder = None

            if metadata_key not in metadata_types:
                raise UsageError('Unexpected metadata key: %s' % (metadata_key,))
            metadata_type = metadata_types[metadata_key]

            if metadata_type == list:
                remainders = remainder.split() if remainder else []
                if any(unicode_util.contains_unicode(r) for r in remainders):
                    raise UsageError(
                        'Metadata cannot contain unicode: %s = %s' % (metadata_key, remainder)
                    )
                result[metadata_key] = remainders
            elif metadata_type == str:
                if remainder is not None and unicode_util.contains_unicode(remainder):
                    raise UsageError(
                        'Metadata cannot contain unicode: %s = %s' % (metadata_key, remainder)
                    )
                result[metadata_key] = remainder
            else:
                try:
                    result[metadata_key] = (
                        metadata_type(remainder) if remainder is not None else None
                    )
                except Exception:
                    raise UsageError('Invalid value %s for type %s' % (remainder, metadata_type))
    if 'name' not in result:
        raise UsageError('No name specified; aborting')
    return result

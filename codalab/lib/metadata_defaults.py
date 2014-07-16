'''
MetadataDefaults is a registry of functions used to compute default metadata
values for new bundles created using the command-line client.

The logic being done here combines information about the bundle subclass and
information from command-line arguments, so it doesn't belong in the core
bundle subclass code.
'''
import os
import platform

from codalab.bundles import UPLOADED_TYPES
from codalab.bundles.make_bundle import MakeBundle
from codalab.bundles.run_bundle import RunBundle
from codalab.lib import path_util


class MetadataDefaults(object):
    @staticmethod
    def get_anonymous_name(bundle_subclass):
        #return 'anonymous-' + bundle_subclass.BUNDLE_TYPE
        return '(none)'

    @staticmethod
    def get_default(spec, bundle_subclass, args):
        fn_name = 'get_default_%s' % (spec.key,)
        fn = getattr(MetadataDefaults, fn_name, None)
        if fn:
            return fn(bundle_subclass, args)
        result = spec.get_constructor()()
        # We need to return a list instead of a set because command-line values for
        # set metadata objects must be JSON-able. When the metadata is marshalled
        # into the database, it will be converted into a set.
        return list(result) if type(result) == set else []

    @staticmethod
    def get_default_name(bundle_subclass, args):
        if hasattr(args, 'path'):
            items = []
            for path in args.path:
                absolute_path = path_util.normalize(path)
                items.append(os.path.basename(absolute_path))
            return '-'.join(items)
        elif bundle_subclass is MakeBundle:
            if len(args.target_spec) == 1 and ':' not in args.target_spec[0]:
                return os.path.basename(args.target_spec[0])
        return MetadataDefaults.get_anonymous_name(bundle_subclass)

    @staticmethod
    def get_default_description(bundle_subclass, args):
        if bundle_subclass.BUNDLE_TYPE in UPLOADED_TYPES:
            description = ' '.join(path_util.normalize(path) for path in args.path)
            return 'Upload %s' % (description,)
        elif bundle_subclass is MakeBundle:
            return 'Make {%s}' % (', '.join(args.target_spec))
        elif bundle_subclass is RunBundle:
            return 'Run {%s}[%s]' % (', '.join(args.target_spec), args.command)
        return ''

    @staticmethod
    def get_default_architectures(bundle_subclass, args):
        return [platform.machine()] if platform.machine() else []

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
from codalab.bundles.uploaded_bundle import UploadedBundle
from codalab.lib import path_util, spec_util
from codalab.common import UsageError

class MetadataDefaults(object):
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
        return list(result) if type(result) == set else result

    @staticmethod
    def get_default_name(bundle_subclass, args):
        if issubclass(bundle_subclass, UploadedBundle):
            items = []
            for path in args.path:
                absolute_path = path_util.normalize(path)
                items.append(os.path.basename(absolute_path))
            return spec_util.create_default_name(None, '-'.join(items))
        elif bundle_subclass is MakeBundle:
            if len(args.target_spec) == 1 and ':' not in args.target_spec[0]:  # direct link
                return os.path.basename(args.target_spec[0])
            else:  # multiple targets
                name = ' '.join(args.target_spec)
                return spec_util.create_default_name(bundle_subclass.BUNDLE_TYPE, str(name))
        elif bundle_subclass is RunBundle:
            return spec_util.create_default_name(bundle_subclass.BUNDLE_TYPE, args.command)
        else:
            raise UsageError("Unhandled class: %s" % bundle_subclass)

    @staticmethod
    def get_default_description(bundle_subclass, args):
        return ''

    @staticmethod
    def get_default_architectures(bundle_subclass, args):
        return [platform.machine()] if platform.machine() else []

'''
DerivedBundle is an abstract Bundle supertype for bundles that need to be
run by CodaLab and that have dependencies (i.e. make and run bundles).
'''
from typing import List

from codalab.bundles.named_bundle import NamedBundle
from codalab.common import UsageError
from codalab.lib import spec_util
from codalab.objects.metadata_spec import MetadataSpec


class DerivedBundle(NamedBundle):
    METADATA_SPECS = list(NamedBundle.METADATA_SPECS)  # type: List
    METADATA_SPECS.append(
        MetadataSpec(
            'allow_failed_dependencies',
            bool,
            'Whether to allow this bundle to have failed or killed dependencies (allow_failed_dependencies).',
            default=False,
            lock_after_start=True,
        )
    )

    @classmethod
    def construct(cls, targets, command, metadata, owner_id, uuid, data_hash, state):
        if not uuid:
            uuid = spec_util.generate_uuid()
        # Check that targets does not include both keyed and anonymous targets.
        if len(targets) > 1 and any(key == '' for key, value in targets):
            raise UsageError('Must specify keys when packaging multiple targets!')

        # List the dependencies of this bundle on its targets.
        dependencies = []
        for (child_path, (parent_uuid, parent_path)) in targets:
            dependencies.append(
                {
                    'child_uuid': uuid,
                    'child_path': child_path,
                    'parent_uuid': parent_uuid,
                    'parent_path': parent_path,
                }
            )
        return super(DerivedBundle, cls).construct(
            {
                'uuid': uuid,
                'bundle_type': cls.BUNDLE_TYPE,
                'command': command,
                'data_hash': data_hash,
                'state': state,
                'metadata': metadata,
                'dependencies': dependencies,
                'owner_id': owner_id,
            }
        )

'''
MakeBundle is a Bundle type that symlinks a number of targets in from other
bundles to produce a new, packaged bundle.
'''
from typing import List

from codalab.bundles.derived_bundle import DerivedBundle
from codalab.worker.bundle_state import State
from codalab.objects.metadata_spec import MetadataSpec


class MakeBundle(DerivedBundle):
    BUNDLE_TYPE = 'make'
    METADATA_SPECS = list(DerivedBundle.METADATA_SPECS)  # type: List
    METADATA_SPECS.append(
        MetadataSpec(
            'staged_status',
            str,
            'Information about the status of the staged bundle (staged_status).',
            generated=True,
        )
    )

    @classmethod
    def construct(
        cls, targets, command, metadata, owner_id, uuid=None, data_hash=None, state=State.CREATED
    ):
        return super(MakeBundle, cls).construct(
            targets, command, metadata, owner_id, uuid, data_hash, state
        )

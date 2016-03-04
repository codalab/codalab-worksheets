'''
MakeBundle is a Bundle type that symlinks a number of targets in from other
bundles to produce a new, packaged bundle.
'''
from codalab.bundles.derived_bundle import DerivedBundle
from codalab.common import State


class MakeBundle(DerivedBundle):
    BUNDLE_TYPE = 'make'
    METADATA_SPECS = list(DerivedBundle.METADATA_SPECS)

    @classmethod
    def construct(cls, targets, command, metadata, owner_id, uuid=None, data_hash=None, state=State.CREATED):
        return super(MakeBundle, cls).construct(targets, command, metadata, owner_id, uuid, data_hash, state)

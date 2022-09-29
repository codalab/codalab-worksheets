'''
UploadedBundle is a abstract Bundle supertype that represents a bundle that is
directly uploaded to the bundle store. Uploaded bundles are constructed with
just a bundle store data hash and a metadata dict, and never need to be run.
'''
from typing import List

from codalab.bundles.named_bundle import NamedBundle
from codalab.objects.metadata_spec import MetadataSpec
from codalab.worker.bundle_state import State


class UploadedBundle(NamedBundle):
    METADATA_SPECS = list(NamedBundle.METADATA_SPECS)  # type: List
    METADATA_SPECS.append(
        MetadataSpec(
            'license', str, 'The license under which this program/dataset is released (license).',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'source_url',
            str,
            'URL corresponding to the original source of this bundle (source_url).',
        )
    )
    METADATA_SPECS.append(
        MetadataSpec('link_url', str, 'Link URL of bundle (link_url).', optional=True,)
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'link_format',
            str,
            'Link format of bundle (link_format). Can be equal to "raw" or "zip" (only "raw" is supported as of now).',
            optional=True,
        )
    )
    METADATA_SPECS.append(
        MetadataSpec(
            'store',
            str,
            'The name of the bundle store where the bundle should be uploaded to (store). If unspecified, an optimal available bundle store will be chosen.',
            default=None,
            hidden=True,
            optional=True,
        )
    )

    @classmethod
    def construct(cls, metadata, owner_id, uuid=None):
        row = {
            'bundle_type': cls.BUNDLE_TYPE,
            'command': None,
            'data_hash': None,
            'state': State.READY,
            'metadata': metadata,
            'dependencies': [],
            'owner_id': owner_id,
        }
        if uuid:
            row['uuid'] = uuid
        return super(UploadedBundle, cls).construct(row)

    def run(self, bundle_store, parent_dict):
        assert False, '%ss should never be run!' % (self.__class__.__name__,)

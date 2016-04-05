'''
UploadedBundle is a abstract Bundle supertype that represents a bundle that is
directly uploaded to the bundle store. Uploaded bundles are constructed with
just a bundle store data hash and a metadata dict, and never need to be run.
'''
from codalab.bundles.named_bundle import NamedBundle
from codalab.common import State
from codalab.objects.metadata_spec import MetadataSpec

class UploadedBundle(NamedBundle):
    METADATA_SPECS = list(NamedBundle.METADATA_SPECS)
    METADATA_SPECS.append(MetadataSpec('license', basestring, 'The license under which this program/dataset is released.'))
    METADATA_SPECS.append(MetadataSpec('source_url', basestring, 'URL corresponding to the original source of this bundle.'))

    @classmethod
    def construct(cls, metadata, owner_id, uuid=None):
        row = {
          'bundle_type': cls.BUNDLE_TYPE,
          'command': None,
          'data_hash': None,
          'state': State.READY,
          'metadata': metadata,
          'dependencies': [],
          'owner_id': owner_id
        }
        if uuid:
            row['uuid'] = uuid
        return super(UploadedBundle, cls).construct(row)

    def run(self, bundle_store, parent_dict):
        assert(False), '%ss should never be run!' % (self.__class__.__name__,)

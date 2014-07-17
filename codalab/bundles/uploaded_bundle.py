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
    METADATA_SPECS.append(MetadataSpec('license', basestring, 'which license this program/data is released under'))
    METADATA_SPECS.append(MetadataSpec('source_url', basestring, 'where this data came from'))

    @classmethod
    def construct(cls, data_hash, metadata, uuid=None):
        row = {
          'bundle_type': cls.BUNDLE_TYPE,
          'command': None,
          'data_hash': data_hash,
          'state': State.READY,
          'metadata': metadata,
          'dependencies': [],
        }
        if uuid:
            row['uuid'] = uuid
        return super(UploadedBundle, cls).construct(row)

    def get_hard_dependencies(self):
        # Uploaded bundles don't have any dependencies on other bundles at all.
        return []

    def run(self, bundle_store, parent_dict):
        assert(False), '%ss should never be run!' % (self.__class__.__name__,)

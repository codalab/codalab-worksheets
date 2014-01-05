'''
UploadedBundle is a abstract Bundle supertype that represents a bundle that is
directly uploaded to the bundle store. Uploaded bundles are constructed with
just a bundle store data hash and a metadata dict, and never need to be run.
'''
from codalab.bundles.named_bundle import NamedBundle
from codalab.common import State


class UploadedBundle(NamedBundle):
  @classmethod
  def construct(cls, data_hash, metadata):
    return cls({
      'bundle_type': cls.BUNDLE_TYPE,
      'command': None,
      'data_hash': data_hash,
      'state': State.READY,
      'metadata': metadata,
      'dependencies': [],
    })

  def run(self, bundle_store, parent_dict):
    assert(False), '%ss should never be run!' % (self.__class__.__name__,)

from codalab.bundles.named_bundle import NamedBundle
from codalab.common import State


class UploadedBundle(NamedBundle):
  @classmethod
  def construct(cls, data_hash, metadata):
    return cls({
      'bundle_type': cls.BUNDLE_TYPE,
      'data_hash': data_hash,
      'state': State.READY,
      'metadata': metadata,
      'dependencies': [],
    })

  def run(self, bundle_store, parent_dict):
    assert(False), '%ss should never be run!' % (self.__class__.__name__,)

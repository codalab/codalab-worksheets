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
    })

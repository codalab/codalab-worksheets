import os
import re
import uuid

from codalab.common import (
  precondition,
  UsageError,
)
from codalab.model.database_object import DatabaseObject
from codalab.objects.dependency import Dependency
from codalab.objects.metadata import Metadata


class Bundle(DatabaseObject):
  COLUMNS = ('uuid', 'bundle_type', 'command', 'data_hash', 'state')
  UUID_REGEX = re.compile('^0x[0-9a-f]{32}\Z')

  # Bundle subclasses should have the following class-level attributes:
  #   - BUNDLE_TYPE: a string bundle type
  #   - METADATA_SPECS: a list of MetadataSpec objects
  BUNDLE_TYPE = None
  METADATA_SPECS = None

  @classmethod
  def construct(cls, *args, **kwargs):
    raise NotImplementedError

  @staticmethod
  def generate_uuid():
    return '0x%s' % (uuid.uuid4().hex,)

  @classmethod
  def check_uuid(cls, uuid):
    '''
    Raise a PreconditionViolation if the uuid does not conform to its regex.
    '''
    malformed_uuid = 'uuids must match %s, was %s' % (
      cls.UUID_REGEX.pattern,
      uuid,
    )
    precondition(cls.UUID_REGEX.match(uuid), malformed_uuid)

  def validate(self):
    '''
    Check a number of basic conditions that would indicate serious errors if
    they do not hold. Subclasses may override this method for further
    validation, but they should always call the super's method.
    '''
    self.check_uuid(self.uuid)
    abstract_init = 'init-ed abstract bundle: %s' % (self.__class__.__name__,)
    precondition(self.BUNDLE_TYPE, abstract_init)
    type_mismatch = 'Mismatch: %s vs %s' % (self.bundle_type, self.BUNDLE_TYPE)
    precondition(self.bundle_type == self.BUNDLE_TYPE, type_mismatch)
    # Check that metadata conforms to specs and check each dependency.
    self.metadata.validate(self.METADATA_SPECS)
    for dep in self.dependencies:
      dep.validate()

  def __repr__(self):
    return '%s(uuid=%r, name=%r)' % (
      self.__class__.__name__,
      str(self.uuid),
      str(self.metadata.name),
    )

  def update_in_memory(self, row, strict=False):
    metadata = row.pop('metadata', None)
    dependencies = row.pop('dependencies', None)
    if strict:
      precondition(metadata is not None, 'No metadata: %s' % (row,))
      precondition(dependencies is not None, 'No dependencies: %s' % (row,))
      if 'uuid' not in row:
        row['uuid'] = self.generate_uuid()
    super(Bundle, self).update_in_memory(row)
    if metadata is not None:
      self.metadata = Metadata(self.METADATA_SPECS, metadata)
    if dependencies is not None:
      self.dependencies = [Dependency(dep) for dep in dependencies]
  
  def to_dict(self):
    result = super(Bundle, self).to_dict()
    result['metadata'] = self.metadata.to_dicts(self.METADATA_SPECS)
    for metadata_row in result['metadata']:
      metadata_row['bundle_uuid'] = self.uuid
    result['dependencies'] = [dep.to_dict() for dep in self.dependencies]
    return result

  def run(self, bundle_store, parent_dict):
    '''
    Perform the computation needed to construct this bundle, upload the result
    to the given BundleStore, and return its new data hash.

    parent_dict should be a dictionary mapping uuids -> bundles for each uuid
    that this bundle depends on.
    '''
    raise NotImplementedError

  def install_dependencies(self, bundle_store, parent_dict, path, rel):
    '''
    Symlink this bundle's dependencies into the directory at path.
    The caller is responsible for cleaning up this directory.
    '''
    precondition(os.path.isabs(path), '%s is a relative path!' % (path,))
    precondition(os.path.isdir(path), '%s is not a directory!' % (path,))
    for dep in self.dependencies:
      parent = parent_dict[dep.parent_uuid]
      # Compute an absolute target and check that the dependency exists.
      target = os.path.join(
        bundle_store.get_location(parent.data_hash),
        dep.parent_path,
      )
      if not os.path.exists(target):
        raise UsageError('Target %s not found!' % (target,))
      if rel:
        # Create a symlink that points to the dependency's relative target.
        target = os.path.join(
          os.pardir,
          bundle_store.get_location(parent.data_hash, relative=True),
          dep.parent_path,
        )
      link_path = os.path.join(path, dep.child_path)
      os.symlink(target, link_path)

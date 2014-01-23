'''
Bundle is the ORM class for an individual bundle in the bundle system.
This class overrides the ORMObject serialization methods, because a single
bundle is stored in the database as one row in in the bundle table, plus
multiple rows in the metadata and dependency tables.

Each bundle type is implemented in a subclass of this class. These subclasses
must set their BUNDLE_TYPE and METADATA_SPEC class attributes. In addition,
they may override a number of methods of the base class:
  construct: different bundle subclass might take different parameters
  validate: bundle subclasses may require additional validation
  run: bundle subclasses that must be executed must override this method

The base class provides one method, install_dependencies, that may be useful
when implementing the run method.
'''
import os
import re
import uuid

from codalab.common import (
  precondition,
  UsageError,
)
from codalab.lib import path_util
from codalab.model.orm_object import ORMObject
from codalab.objects.dependency import Dependency
from codalab.objects.metadata import Metadata


class Bundle(ORMObject):
  COLUMNS = ('uuid', 'bundle_type', 'command', 'data_hash', 'state')
  UUID_REGEX = re.compile('^0x[0-9a-f]{32}\Z')
  UUID_PREFIX_REGEX = re.compile('^0x[0-9a-f]{1,31}\Z')

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

  @classmethod
  def get_user_defined_metadata(cls):
    '''
    Return a list of metadata specs for metadata that must be input by the user.
    '''
    return [spec for spec in cls.METADATA_SPECS if not spec.generated]

  def install_dependencies(self, bundle_store, parent_dict, path, rel):
    '''
    Symlink this bundle's dependencies into the directory at path.
    The caller is responsible for cleaning up this directory.
    '''
    precondition(os.path.isabs(path), '%s is a relative path!' % (path,))
    for dep in self.dependencies:
      parent = parent_dict[dep.parent_uuid]
      # Compute an absolute target and check that the dependency exists.
      target = path_util.safe_join(
        bundle_store.get_location(parent.data_hash),
        dep.parent_path,
      )
      if not os.path.exists(target):
        parent_spec = getattr(parent.metadata, 'name', parent.uuid)
        target_text = path_util.safe_join(parent_spec, dep.parent_path)
        raise UsageError('Target not found: %s' % (target_text,))
      if rel:
        # Create a symlink that points to the dependency's relative target.
        target = path_util.safe_join(
          (os.pardir if dep.child_path else ''),
          bundle_store.get_location(parent.data_hash, relative=True),
          dep.parent_path,
        )
      link_path = path_util.safe_join(path, dep.child_path)
      os.symlink(target, link_path)

  def get_hard_dependencies(self):
    '''
    Returns a list of dependencies that are actually symlinked into this bundle
    at the time that it is uploaded to the bundle store.
    '''
    raise NotImplementedError

  def run(self, bundle_store, parent_dict):
    '''
    Perform the computation needed to construct this bundle, upload the result
    to the given bundle store, and return its new data hash.

    parent_dict should be a dictionary mapping uuids -> bundles for each uuid
    that this bundle depends on.
    '''
    raise NotImplementedError

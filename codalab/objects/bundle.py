import re
import uuid

from codalab.common import precondition
from codalab.model.database_object import DatabaseObject
from codalab.model.tables import bundle as cl_bundle
from codalab.objects.dependency import Dependency
from codalab.objects.metadata import Metadata


class Bundle(DatabaseObject):
  TABLE = cl_bundle
  UUID_REGEX = '^0x[0-9a-f]{32}\Z'

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
    malformed_uuid = 'uuid must match %s, is %s' % (cls.UUID_REGEX, uuid)
    precondition(re.match(cls.UUID_REGEX, uuid), malformed_uuid)

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

  def update_in_memory(self, row):
    metadata = row.pop('metadata')
    dependencies = row.pop('dependencies')
    if 'uuid' not in row:
      row['uuid'] = self.generate_uuid()
    super(Bundle, self).update_in_memory(row)
    self.metadata = Metadata(self.METADATA_SPECS, metadata)
    self.dependencies = [Dependency(dep) for dep in dependencies]
  
  def to_dict(self):
    result = super(Bundle, self).to_dict()
    result['metadata'] = self.metadata.to_dicts(self.METADATA_SPECS)
    for metadata_row in result['metadata']:
      metadata_row['bundle_uuid'] = self.uuid
    result['dependencies'] = [dep.to_dict() for dep in self.dependencies]
    return result

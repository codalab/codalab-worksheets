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
'''
from codalab.common import precondition
from codalab.lib import spec_util
from codalab.model.orm_object import ORMObject
from codalab.objects.dependency import Dependency
from codalab.objects.metadata import Metadata


class Bundle(ORMObject):
    COLUMNS = (
        'uuid',
        'bundle_type',
        'command',
        'data_hash',
        'state',
        'owner_id',
        'frozen',
        'is_anonymous',
        'storage_type',
        'is_dir',
    )
    # Bundle subclasses should have the following class-level attributes:
    #   - BUNDLE_TYPE: a string bundle type
    #   - METADATA_SPECS: a list of MetadataSpec objects
    BUNDLE_TYPE: str
    METADATA_SPECS: list

    # Types for columns
    uuid: str
    bundle_type: str
    command: str
    data_hash: str
    state: str
    owner_id: str
    frozen: bool
    is_anonymous: bool
    storage_type: str
    is_dir: bool

    @classmethod
    def construct(cls, *args, **kwargs):
        raise NotImplementedError

    def validate(self):
        '''
        Check a number of basic conditions that would indicate serious errors if
        they do not hold. Subclasses may override this method for further
        validation, but they should always call the super's method.
        '''
        spec_util.check_uuid(self.uuid)
        abstract_init = 'init-ed abstract bundle: %s' % (self.__class__.__name__,)
        precondition(self.BUNDLE_TYPE, abstract_init)
        type_mismatch = 'Mismatch: %s vs %s' % (self.bundle_type, self.BUNDLE_TYPE)
        precondition(self.bundle_type == self.BUNDLE_TYPE, type_mismatch)
        # Check that metadata conforms to specs and check each dependency.
        self.metadata.validate(self.METADATA_SPECS)
        for dep in self.dependencies:
            dep.validate()

    def __repr__(self):
        return '%s(uuid=%r)' % (self.__class__.__name__, str(self.uuid))

    def update_in_memory(self, row, strict=False):
        metadata = row.pop('metadata', None)
        dependencies = row.pop('dependencies', None)
        if strict:
            precondition(metadata is not None, 'No metadata: %s' % (row,))
            precondition(dependencies is not None, 'No dependencies: %s' % (row,))
            if 'uuid' not in row:
                row['uuid'] = spec_util.generate_uuid()
        super(Bundle, self).update_in_memory(row)
        if metadata is not None:
            self.metadata = Metadata(self.METADATA_SPECS, metadata)
        if dependencies is not None:
            self.dependencies = [Dependency(dep) for dep in dependencies]

    def to_dict(self, strict=True):
        result = super(Bundle, self).to_dict(strict=strict)
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

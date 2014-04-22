'''
Defines ORM classes for groups and permissions.
'''
from codalab.model.orm_object import ORMObject
from codalab.common import (
    precondition,
)
from codalab.lib import (
    spec_util,
)

class Group(ORMObject):
    '''
    Defines a group object which is used to assign permissions to a set of users.
    '''
    COLUMNS = ('uuid', 'name', 'owner_id', 'user_defined')

    def validate(self):
        '''
        Check a number of basic conditions that would indicate serious errors if
        they do not hold. Right now, validation only checks this worksheet's uuid
        and its name.
        '''
        spec_util.check_uuid(self.uuid)
        spec_util.check_name(self.name)
        precondition(type(self.owner_id) == int and self.owner_id >= 0, 'Invalid value: owner_id.')
        precondition(type(self.user_defined) == bool, 'Invalid value: user_defined.')

    def __repr__(self):
        return 'Group(uuid=%r, name=%r)' % (self.uuid, self.name)

    def update_in_memory(self, row, strict=False):
        if strict:
            if 'uuid' not in row:
                row['uuid'] = spec_util.generate_uuid()
        super(Group, self).update_in_memory(row)

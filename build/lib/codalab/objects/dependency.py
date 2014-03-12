'''
Dependency is the ORMObject wrapper around rows of the dependency table.
'''
import re

from codalab.common import UsageError
from codalab.lib import spec_util
from codalab.model.orm_object import ORMObject


class Dependency(ORMObject):
    COLUMNS = ('child_uuid', 'child_path', 'parent_uuid', 'parent_path')
    CHILD_PATH_REGEX = re.compile('^[a-zA-Z0-9_\-.]*\Z')

    def validate(self):
        spec_util.check_uuid(self.child_uuid)
        spec_util.check_uuid(self.parent_uuid)
        if not self.CHILD_PATH_REGEX.match(self.child_path):
            raise UsageError(
              'child_subpath must match %s, was %s' %
              (self.CHILD_PATH_REGEX.pattern, self.child_path)
            )

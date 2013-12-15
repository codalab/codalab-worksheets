import re

from codalab.common import UsageError
from codalab.model.database_object import DatabaseObject
from codalab.model.tables import dependency as cl_dependency


class Dependency(DatabaseObject):
  TABLE = cl_dependency

  CHILD_PATH_REGEX = re.compile('^[a-zA-Z0-9_\-.]+\Z')

  def validate(self):
    from codalab.objects.bundle import Bundle
    Bundle.check_uuid(self.child_uuid)
    Bundle.check_uuid(self.parent_uuid)
    if not self.CHILD_PATH_REGEX.match(self.child_path):
      raise UsageError(
        'child_subpath must match %s, was %s' %
        (self.CHILD_PATH_REGEX.pattern, self.child_path)
      )

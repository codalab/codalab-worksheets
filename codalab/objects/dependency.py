import re

from codalab.model.database_object import DatabaseObject
from codalab.model.tables import dependency as cl_dependency


class Dependency(DatabaseObject):
  TABLE = cl_dependency

  CHILD_PATH_REGEX = '^[a-zA-Z0-9_-]+\Z'

  def validate(self):
    from codalab.objects.bundle import Bundle
    Bundle.check_uuid(self.child_uuid)
    Bundle.check_uuid(self.parent_uuid)
    if not re.match(self.CHILD_PATH_REGEX, self.child_path):
      raise ValueError(
        'child_subpath should match %s, was %s' %
        (self.CHILD_PATH_REGEX, self.child_path)
      )

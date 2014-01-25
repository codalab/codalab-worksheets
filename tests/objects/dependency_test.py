import unittest

from codalab.model.tables import bundle_dependency as cl_bundle_dependency
from codalab.objects.dependency import Dependency


class DependencyTest(unittest.TestCase):
  COLUMNS = tuple(col.name for col in cl_bundle_dependency.c if col.name != 'id')

  def test_columns(self):
    '''
    Test that Dependency.COLUMNS includes precisely the non-id columns of
    cl_dependency, in the same order.
    '''
    self.assertEqual(Dependency.COLUMNS, self.COLUMNS)

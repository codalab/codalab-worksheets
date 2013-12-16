import unittest

from codalab.model.tables import dependency as cl_dependency
from codalab.objects.dependency import Dependency


class DependencyTest(unittest.TestCase):
  COLUMNS = tuple(col.name for col in cl_dependency.c if col.name != 'id')

  def test_columns(self):
    '''
    Test that Dependency.COLUMNS includes precisely the non-id columns of
    cl_dependency, in the same order.
    '''
    self.assertEqual(Dependency.COLUMNS, self.COLUMNS)

import errno
import hashlib
import mock
import os
import unittest

from codalab.common import PreconditionViolation
from codalab.lib import worksheet_util

class WorksheetUtilTest(unittest.TestCase):
  def test_apply_func(self):
    '''
    Test apply_func for rendering values in worksheets.
    '''
    self.assertEqual(worksheet_util.apply_func(None, 'hello'), 'hello')
    self.assertEqual(worksheet_util.apply_func('[1:2]', 'hello'), 'e')
    self.assertEqual(worksheet_util.apply_func('[:2]', 'hello'), 'he')
    self.assertEqual(worksheet_util.apply_func('[2:]', 'hello'), 'llo')
    self.assertEqual(worksheet_util.apply_func('date', '1427467247')[:10], '2015-03-27')  # Don't test time because of time zones
    self.assertEqual(worksheet_util.apply_func('duration', '63'), '1m3s')
    self.assertEqual(worksheet_util.apply_func('size', '1024'), '1k')
    self.assertEqual(worksheet_util.apply_func('s/a/b', 'aa'), 'bb')
    self.assertEqual(worksheet_util.apply_func(r's/(.+)\/(.+)/\2\/\1', '3/10'), '10/3')
    self.assertEqual(worksheet_util.apply_func('%.2f', '1.2345'), '1.23')

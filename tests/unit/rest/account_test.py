import unittest
from mock import Mock
from codalab.server.rest_server import create_rest_app
from webtest import TestApp

class AccountTest(unittest.TestCase):
    def setUp(self):
      self.app = TestApp(create_rest_app())

    def test_account_css(self):
      print(self.app.get('/rest/account/css'))
import unittest
from mock import Mock
from bottle import request
from codalab.server.rest_server import request, create_rest_app
from webtest import TestApp
import os

class UserTest(unittest.TestCase):
    def setUp(self):
      self.app = TestApp(create_rest_app())

    def test_user_unauthenticated(self):
      os.environ["CODALAB_TEST_USER"] = ""
      response = self.app.get('/rest/user')
      self.assertEqual(response.status_int, 302)
      self.assertEqual(response.headers["Location"], "http://localhost:80/account/login?next=http%3A%2F%2Flocalhost%3A80%2Frest%2Fuser")
    
    def test_user_authenticated(self):
      os.environ["CODALAB_TEST_USER"] = "codalab"
      response = self.app.get('/rest/user')
      self.assertEqual(response.status_int, 200)
      # self.assertEqual(response.body, 200)
      os.environ["CODALAB_TEST_USER"] = ""


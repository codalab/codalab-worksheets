import unittest
from mock import Mock
from bottle import request
from codalab.server.rest_server import request, create_rest_app
from webtest import TestApp
import os
import json

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
      self.assertEqual(json.loads(response.body.decode()), {"data": {"type": "users", "attributes": {"email": "", "first_name": "", "date_joined": "Tue Jul  7 22:10:30 2020", "parallel_run_quota": 100, "last_name": "", "time_quota": 3153600000, "disk_quota": 107374000000, "last_login": "Thu Jul 16 14:53:38 2020", "url": None, "notifications": 2, "user_name": "codalab", "time_used": 1, "affiliation": "", "disk_used": 5573520}, "id": "0"}, "meta": {"version": "0.5.21"}})
      os.environ["CODALAB_TEST_USER"] = ""


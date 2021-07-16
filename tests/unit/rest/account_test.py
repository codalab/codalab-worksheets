import os
import random
import urllib
from .base import BaseTestCase


class UserTest(BaseTestCase):
    def test_user_signup(self):
        os.environ["CODALAB_TEST_USER"] = ""
        email = "test@test.com{}".format(random.getrandbits(16))
        username = "test{}".format((random.getrandbits(16)))
        body = {
            'email': email,
            'username': username,
            'first_name': "test",
            'last_name': "test",
            'affiliation': "None",
            'password': "testtest",
            'confirm_password': "testtest",
            'success_uri': "/account/signup/success",
            'error_uri': "/account/signup",
            'token': 'test',
        }
        response = self.app.post('/rest/account/signup', body)
        self.assertEqual(response.status_int, 302)
        self.assertEqual(
            response.headers["Location"],
            "http://localhost:80/account/signup/success?{}".format(
                urllib.parse.urlencode({"email": email})
            ),
        )

    def test_missing_token(self):
        os.environ["CODALAB_TEST_USER"] = ""
        email = "test@test.com{}".format(random.getrandbits(16))
        username = "test{}".format((random.getrandbits(16)))
        body = {
            'email': email,
            'username': username,
            'first_name': "test",
            'last_name': "test",
            'affiliation': "None",
            'password': "testtest",
            'confirm_password': "testtest",
            'success_uri': "/account/signup/success",
            'error_uri': "/account/signup",
        }
        response = self.app.post('/rest/account/signup', body)
        self.assertEqual(response.status_int, 302)
        self.assertTrue('error=Google+reCAPTCHA+token+is+missing.' in response.headers["Location"])

    def test_user_signup_fails_when_already_logged_in(self):
        """User signup should fail when the user is already logged in."""
        os.environ["CODALAB_TEST_USER"] = "codalab"
        email = "test@test.com{}".format(random.getrandbits(16))
        username = "test{}".format((random.getrandbits(16)))
        body = {
            'email': email,
            'username': username,
            'first_name': "test",
            'last_name': "test",
            'affiliation': "None",
            'password': "testtest",
            'confirm_password': "testtest",
            'success_uri': "/account/signup/success",
            'error_uri': "/account/signup",
            'token': 'test',
        }
        response = self.app.post('/rest/account/signup', body)
        self.assertEqual(response.status_int, 302)
        self.assertIn("/account/signup?", response.headers["Location"])
        self.assertIn("already+logged+in", response.headers["Location"])

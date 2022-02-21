import os
from .base import BaseTestCase


class UserTest(BaseTestCase):
    def test_user_unauthenticated(self):
        os.environ["CODALAB_TEST_USER"] = ""
        response = self.app.get('/rest/user')
        self.assertEqual(response.status_int, 302)
        self.assertEqual(
            response.headers["Location"],
            "http://localhost:80/account/login?next=http%3A%2F%2Flocalhost%3A80%2Frest%2Fuser",
        )

    def test_user_authenticated(self):
        os.environ["CODALAB_TEST_USER"] = "codalab"
        response = self.app.get('/rest/user')
        self.assertEqual(response.status_int, 200)
        data = response.json["data"]
        # These variables can change due to other tests.
        del data["attributes"]["disk_used"]
        del data["attributes"]["time_used"]
        del data["attributes"]["date_joined"]
        del data["attributes"]["last_login"]
        del data["attributes"]["disk_quota"]
        del data["attributes"]["time_quota"]
        self.assertEqual(
            data,
            {
                "type": "users",
                "attributes": {
                    "email": "",
                    "first_name": "",
                    "parallel_run_quota": 100,
                    "last_name": "",
                    "url": None,
                    "notifications": 2,
                    "user_name": "codalab",
                    "url": None,
                    "parallel_run_quota": 100,
                    'protected_mode': False,
                    "avatar_id": None,
                    'is_root_user': True,
                },
                "id": "0",
            },
        )
        os.environ["CODALAB_TEST_USER"] = ""

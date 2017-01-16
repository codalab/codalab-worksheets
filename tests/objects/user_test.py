import unittest
import datetime

from codalab.objects.user import User
from codalab.model.tables import NOTIFICATIONS_IMPORTANT

user = User({
    "user_id": 1,
    "user_name": "test",
    "email": "test@test.com",
    "notifications": NOTIFICATIONS_IMPORTANT,
    "last_login": datetime.datetime.now(),
    "is_active": True,
    "first_name": None,
    "last_name": None,
    "date_joined": datetime.datetime.now(),
    "is_verified": True,
    "is_superuser": False,
    "password": "",
    "time_quota": 0,
    "time_used": 0,
    "disk_quota": 0,
    "disk_used": 0,
    "affiliation": None,
    "url": None,
})


class UserTest(unittest.TestCase):
    def test_hashing(self):
        """
        Test that hashing works
        """
        password = "vErY_s3cur3_pa$$word"
        user.set_password(password)
        self.assertTrue(user.check_password(password))
        self.assertFalse(user.check_password(password + 'a'))
        self.assertFalse(user.check_password(''))

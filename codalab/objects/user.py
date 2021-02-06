"""
User objects representing rows from the user table
"""
import base64

from codalab.common import UsageError
from codalab.lib import formatting
from codalab.lib.crypt_util import force_bytes, get_random_string, pbkdf2, constant_time_compare
from codalab.model.orm_object import ORMObject
from codalab.model.tables import NOTIFICATIONS_NONE


class User(ORMObject):
    COLUMNS = (
        'user_id',
        'user_name',
        'email',
        'notifications',
        'last_login',
        'is_active',
        'first_name',
        'last_name',
        'date_joined',
        'has_access',
        'is_verified',
        'is_superuser',
        'password',
        'time_quota',
        'parallel_run_quota',
        'time_used',
        'disk_quota',
        'disk_used',
        'affiliation',
        'url',
        'avatar_id',
    )

    PASSWORD_MIN_LENGTH = 8

    @property
    def unique_id(self):
        return self.user_id

    @property
    def name(self):
        return self.user_name

    @property
    def is_authenticated(self):
        return self is not PUBLIC_USER

    @staticmethod
    def encode_password(password, salt, iterations=30000):
        """
        Encode password using the PBKDF2 algorithm.

        :param password: raw password string
        :param salt: salt string
        :param iterations: number of iterations for algorithm
        :return:
        """
        assert password is not None
        assert salt and '$' not in salt
        hash = pbkdf2(password, salt, iterations)
        hash = base64.b64encode(bytes(hash)).decode('ascii').strip()
        return "%s$%d$%s$%s" % ('pbkdf2_sha256', iterations, salt, hash)

    @staticmethod
    def validate_password(password):
        """
        Check if password meets our requirements, raising UsageError if not.
        Requirements:
         - minimum length of 8 characters

        :param password: string password to validate
        :return: None
        """

        if not all(33 <= ord(c) <= 126 for c in password):
            raise UsageError(
                "Password must consist of only printable, non-whitespace ASCII characters."
            )

        if len(password) < User.PASSWORD_MIN_LENGTH:
            raise UsageError(
                "Password must contain at least %d characters." % User.PASSWORD_MIN_LENGTH
            )

    def set_password(self, password):
        """
        Save password to user.
        :param password: string of new password
        :return: None
        """
        self.password = self.encode_password(password, get_random_string())

    def check_password(self, password):
        """
        Returns True iff password matches the user's current password.

        :param password: string of password to check
        :return: boolean
        """
        if not self.password:
            return False

        algorithm, iterations, salt, _ = self.password.split('$', 3)
        assert algorithm == 'pbkdf2_sha256'
        encoded = self.encode_password(password, salt, int(iterations))
        return constant_time_compare(force_bytes(self.password), force_bytes(encoded))

    def check_quota(self, need_time=False, need_disk=False):
        if need_time:
            if self.time_used >= self.time_quota:
                raise UsageError(
                    'Out of time quota: %s'
                    % formatting.ratio_str(formatting.duration_str, self.time_used, self.time_quota)
                )
        if need_disk:
            if self.disk_used >= self.disk_quota:
                raise UsageError(
                    'Out of disk quota: %s'
                    % formatting.ratio_str(formatting.size_str, self.disk_used, self.disk_quota)
                )

    def __str__(self):
        return "%s(%s)" % (self.user_name, self.user_id)


PUBLIC_USER = User(
    {
        "user_id": None,  # sentinel for BundleModel methods indicating public user
        "user_name": 'public',
        "email": None,
        "notifications": NOTIFICATIONS_NONE,
        "last_login": None,
        "is_active": True,
        "first_name": None,
        "last_name": None,
        "date_joined": None,
        "has_access": False,
        "is_verified": True,
        "is_superuser": False,
        "password": None,
        "time_quota": 0,
        "parallel_run_quota": 0,
        "time_used": 0,
        "disk_quota": 0,
        "disk_used": 0,
        "affiliation": None,
        "url": None,
        "avatar_id": None,
    }
)

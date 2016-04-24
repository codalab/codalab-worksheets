"""
User objects representing rows from the user table
"""
import base64

from codalab.common import UsageError
from codalab.model.orm_object import ORMObject
from codalab.lib.crypt_util import force_bytes, get_random_string, pbkdf2, constant_time_compare


class User(ORMObject):
    COLUMNS = ('user_id', 'user_name', 'email', 'last_login', 'is_active', 'first_name', 'last_name', 'date_joined',
               'is_verified', 'is_superuser', 'password', 'time_quota', 'time_used', 'disk_quota', 'disk_used',
               'affiliation', 'url')

    PASSWORD_MIN_LENGTH = 8

    @property
    def unique_id(self):
        return self.user_id

    @property
    def name(self):
        return self.user_name

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
        hash = base64.b64encode(hash).decode('ascii').strip()
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
            raise UsageError("Password must consist of only printable, non-whitespace ASCII characters.")

        if len(password) < User.PASSWORD_MIN_LENGTH:
            raise UsageError("Password must contain at least %d characters." % User.PASSWORD_MIN_LENGTH)

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


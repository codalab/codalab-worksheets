"""
Dependency is the ORMObject wrapper around rows of the dependency table.
"""
import base64
import hashlib
import hmac

from codalab.common import UsageError
from codalab.model.orm_object import ORMObject
from codalab.lib.server_util import force_bytes, get_random_string


class User(ORMObject):
    COLUMNS = ('user_id', 'user_name', 'email', 'last_login', 'is_active', 'first_name', 'last_name', 'date_joined',
               'is_verified', 'is_superuser', 'password', 'time_quota', 'time_used', 'disk_quota', 'disk_used')

    def validate(self):
        pass

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
        hash = hashlib.pbkdf2_hmac(hashlib.sha256().name, force_bytes(password), force_bytes(salt), iterations)
        hash = base64.b64encode(hash).decode('ascii').strip()
        return "%s$%d$%s$%s" % ('pbkdf2_sha256', iterations, salt, hash)

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
        algorithm, iterations, salt, _ = self.password.split('$', 3)
        assert algorithm == 'pbkdf2_sha256'
        encoded = self.encode_password(password, salt, int(iterations))
        return hmac.compare_digest(force_bytes(self.password), force_bytes(encoded))


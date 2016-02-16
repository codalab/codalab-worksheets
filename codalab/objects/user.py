"""
Dependency is the ORMObject wrapper around rows of the dependency table.
"""
from codalab.common import UsageError
from codalab.model.orm_object import ORMObject


class User(ORMObject):
    COLUMNS = ('user_id', 'user_name', 'email', 'last_login', 'is_active', 'first_name', 'last_name', 'date_joined',
               'is_verified', 'is_superuser', 'password', 'time_quota', 'time_used', 'disk_quota', 'disk_used')

    def validate(self):
        pass

    def set_password(self, password):
        """
        Save password to user.
        :param password: string of new password
        :return: None
        """
        # FIXME: should hash
        self.password = password

    def check_password(self, password):
        """
        Returns True iff password matches the user's current password.

        :param password: string of password to check
        :return: boolean
        """

        # FIXME: should hash
        return self.password == password


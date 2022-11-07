"""
Script that creates the root user.
"""
import sys
import getpass

from codalab.lib import crypt_util
from codalab.lib.codalab_manager import CodaLabManager
from codalab.objects.user import User

sys.path.append('.')
manager = CodaLabManager()
model = manager.model()

username = manager.root_user_name()
user_id = manager.root_user_id()

if len(sys.argv) == 2:
    password = sys.argv[1]
else:
    while True:
        password = getpass.getpass('Password for %s(%s): ' % (username, user_id))
        if getpass.getpass('Confirm password: ') == password:
            break
        print('Passwords don\'t match. Try again.')

if model.get_user(user_id=user_id, check_active=False):
    update = {
        "user_id": user_id,
        "user_name": username,
        "password": User.encode_password(password, crypt_util.get_random_string()),
        "has_access": True,
        "is_active": True,
        "is_verified": True,
    }
    model.update_user_info(update)
else:
    model.add_user(
        username, '', '', '', password, '', user_id=user_id, is_verified=True, has_access=True
    )

#!./venv/bin/python
"""
Script that creates the root user.
"""
import sys
sys.path.append('.')

import getpass

from codalab.lib.codalab_manager import CodaLabManager

manager = CodaLabManager()
model = manager.model()

username = manager.root_user_name()
user_id = manager.root_user_id()
while True:
    password = getpass.getpass()
    if getpass.getpass('Config password: ') == password:
        break

    print 'Passwords don\'t match. Try again.'
    print

model.add_user(username, '', password, user_id, is_verified=True)

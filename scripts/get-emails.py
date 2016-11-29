#!./venv/bin/python
"""
This script prints all the email ids in the user table to sysout line by line
"""

import sys
sys.path.append('.')
from sqlalchemy import select
from codalab.model.tables import user as cl_user
from codalab.lib.codalab_manager import CodaLabManager


manager = CodaLabManager()
model = manager.model()
CODALAB_HOME = manager.codalab_home


with model.engine.begin() as bundle_db:
    # Get set of emails in bundles db
    bundle_emails = bundle_db.execute(select([cl_user.c.email])).fetchall()
    email_list = [x[0] for x in bundle_emails if x[0] != '']
    for email in email_list:
        print(email)

"""add worker_action (e.g., supports kill)

Revision ID: 558ac277c2b9
Revises: 341ee10697f1
Create Date: 2014-08-20 17:30:02.374550

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = '558ac277c2b9'
down_revision = '341ee10697f1'


def upgrade():
    pass


def downgrade():
    op.drop_table('bundle_action')

"""New event table

Revision ID: 54a6e1d6481
Revises: 27bd1de0c078
Create Date: 2015-09-10 12:20:49.269302

"""

# revision identifiers, used by Alembic.
revision = '54a6e1d6481'
down_revision = '27bd1de0c078'

from alembic import op
import sqlalchemy as sa


def upgrade():
    # event_log automatically added
    pass

def downgrade():
    op.drop_table('event')

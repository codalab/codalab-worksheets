"""Add notification column to user table

Revision ID: 98c400adf875
Revises: 730e212b938
Create Date: 2016-10-17 18:00:22.470357

"""

# revision identifiers, used by Alembic.
revision = '98c400adf875'
down_revision = '730e212b938'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('user', sa.Column('send_notifications_flag', sa.Integer, default=0))


def downgrade():
    op.drop_column('user', 'send_notifications_flag')

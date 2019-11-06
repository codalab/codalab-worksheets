"""add last bundle finished time to user table

Revision ID: dc6d60726d9a
Revises: 75d4288ae265
Create Date: 2019-11-06 22:39:59.913605

"""

# revision identifiers, used by Alembic.
revision = 'dc6d60726d9a'
down_revision = '75d4288ae265'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('user', sa.Column('last_bundle_finished_time', sa.DateTime(), nullable=False))


def downgrade():
    op.drop_column('user', 'last_bundle_finished_time')

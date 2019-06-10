"""add parallel run quota

Revision ID: d24522e04593
Revises: 53bcf87ddf40
Create Date: 2019-06-10 21:31:01.650412

"""

# revision identifiers, used by Alembic.
revision = 'd24522e04593'
down_revision = '53bcf87ddf40'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('user', sa.Column('parallel_run_quota', sa.Integer(), nullable=False, server_default='3'))


def downgrade():
    op.drop_column('user', 'parallel_run_quota')

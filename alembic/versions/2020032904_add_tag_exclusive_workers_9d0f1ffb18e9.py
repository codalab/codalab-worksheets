"""Add tag_exclusive workers

Revision ID: 9d0f1ffb18e9
Revises: 75d4288ae265
Create Date: 2020-03-29 04:57:59.567422

"""

# revision identifiers, used by Alembic.
revision = '9d0f1ffb18e9'
down_revision = '75d4288ae265'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('worker', sa.Column('tag_exclusive', sa.Boolean(), nullable=False, server_default=sa.false()))


def downgrade():
    op.drop_column('worker', 'tag_exclusive')

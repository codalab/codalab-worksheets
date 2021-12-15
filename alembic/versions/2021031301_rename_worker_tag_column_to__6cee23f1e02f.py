"""Rename worker tag column to tags

Revision ID: 6cee23f1e02f
Revises: 6c013a88862f
Create Date: 2021-03-13 01:29:29.309976

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '6cee23f1e02f'
down_revision = '6c013a88862f'


def upgrade():
    op.alter_column('worker', 'tag', 'tags')


def downgrade():
    op.alter_column('worker', 'tags', 'tag')

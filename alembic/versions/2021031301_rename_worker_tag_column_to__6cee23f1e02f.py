"""Rename worker tag column to tags

Revision ID: 6cee23f1e02f
Revises: db12798a7cf6
Create Date: 2021-03-13 01:29:29.309976

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '6cee23f1e02f'
down_revision = 'db12798a7cf6'


def upgrade():
    op.add_column('worker', sa.Column('tags', sa.Text(), nullable=True))
    op.drop_column('worker', 'tag')


def downgrade():
    op.add_column('worker', sa.Column('tag', sa.Text(), nullable=True))
    op.drop_column('worker', 'tags')

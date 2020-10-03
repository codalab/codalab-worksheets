"""Add bundle contents index

Revision ID: 40d61632fd13
Revises: 58eccccb346d
Create Date: 2016-03-13 17:00:23.055571

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '40d61632fd13'
down_revision = '58eccccb346d'


def upgrade():
    # bundle_contents_index automatically added
    pass


def downgrade():
    op.drop_table('bundle_contents_index')

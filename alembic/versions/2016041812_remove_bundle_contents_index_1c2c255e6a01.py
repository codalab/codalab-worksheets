"""Remove bundle contents index

Revision ID: 1c2c255e6a01
Revises: 3fe0e77de7a4
Create Date: 2016-04-18 12:54:43.729203

"""

# revision identifiers, used by Alembic.
revision = '1c2c255e6a01'
down_revision = '3fe0e77de7a4'

from alembic import op


def upgrade():
    op.drop_table('bundle_contents_index')


def downgrade():
    pass

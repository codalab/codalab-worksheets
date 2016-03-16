"""Remove bundle contents index

Revision ID: 1c2c255e6a01
Revises: 58ce22fe731e
Create Date: 2016-03-15 12:54:43.729203

"""

# revision identifiers, used by Alembic.
revision = '1c2c255e6a01'
down_revision = '58ce22fe731e'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.drop_table('bundle_contents_index')


def downgrade():
    pass

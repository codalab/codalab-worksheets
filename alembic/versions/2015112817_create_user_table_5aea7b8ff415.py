"""create user table

Revision ID: 5aea7b8ff415
Revises: 4e19ae1a0e64
Create Date: 2015-11-28 17:00:40.509504

"""

# revision identifiers, used by Alembic.
revision = '5aea7b8ff415'
down_revision = '4e19ae1a0e64'

from alembic import op
import sqlalchemy as sa


def upgrade():
    # user automatically added
    pass


def downgrade():
    op.drop_table('user')

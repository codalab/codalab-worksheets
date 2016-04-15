"""Add password reset codes table

Revision ID: 3fe0e77de7a4
Revises: 1799abc91708
Create Date: 2016-04-10 13:38:46.941054

"""

# revision identifiers, used by Alembic.
revision = '3fe0e77de7a4'
down_revision = '1799abc91708'

from alembic import op
import sqlalchemy as sa


def upgrade():
    # reset codes table automatically added
    pass


def downgrade():
    op.drop_table('user_reset_code')

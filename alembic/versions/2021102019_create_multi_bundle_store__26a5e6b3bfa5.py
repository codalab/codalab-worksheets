"""create multi bundle store tables

Revision ID: 26a5e6b3bfa5
Revises: 6c013a88862f
Create Date: 2021-10-20 19:30:20.877152

"""

# revision identifiers, used by Alembic.
revision = '26a5e6b3bfa5'
down_revision = '6c013a88862f'

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

def upgrade():
    # tables automatically added
    pass


def downgrade():
    op.drop_table('bundle_location')
    op.drop_table('bundle_store')

"""make date_created nullable

Revision ID: ddd9989327c7
Revises: 0602bcb323f4
Create Date: 2020-11-20 23:12:26.393224

"""

# revision identifiers, used by Alembic.
revision = 'ddd9989327c7'
down_revision = '0602bcb323f4'

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

def upgrade():
    op.alter_column('worksheet', 'date_created',
               existing_type=mysql.DATETIME(),
               nullable=True)


def downgrade():
    op.alter_column('worksheet', 'date_created',
               existing_type=mysql.DATETIME(),
               nullable=False)

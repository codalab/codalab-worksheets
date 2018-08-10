"""new worksheet properties: title and frozen

Revision ID: 12a9451988cd
Revises: 54a6e1d6481
Create Date: 2015-09-12 14:09:55.317665

"""

# revision identifiers, used by Alembic.
revision = '12a9451988cd'
down_revision = '54a6e1d6481'

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

def upgrade():
    op.add_column('worksheet', sa.Column('frozen', sa.DateTime(), nullable=True))
    op.add_column('worksheet', sa.Column('title', sa.String(length=255), nullable=True))


def downgrade():
    op.drop_column('worksheet', 'title')
    op.drop_column('worksheet', 'frozen')

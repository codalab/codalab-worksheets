"""Remove is_superuser from User table

Revision ID: 22b6c41cd29c
Revises: f720aaefd0b2
Create Date: 2022-10-04 03:42:55.838245

"""

# revision identifiers, used by Alembic.
revision = '22b6c41cd29c'
down_revision = 'f720aaefd0b2'

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('user', 'is_superuser')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('user', sa.Column('is_superuser', mysql.TINYINT(display_width=1), autoincrement=False, nullable=False))
    # ### end Alembic commands ###

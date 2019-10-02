"""Remove action table.

Revision ID: 4bc4499383aa
Revises: 26cf8de18f09
Create Date: 2016-06-06 11:19:31.216291

"""

# revision identifiers, used by Alembic.
revision = '4bc4499383aa'
down_revision = '26cf8de18f09'

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

def upgrade():
    op.drop_table('bundle_action')


def downgrade():
    op.create_table('bundle_action',
    sa.Column('id', mysql.INTEGER(display_width=11), nullable=False),
    sa.Column('bundle_uuid', mysql.VARCHAR(length=63), nullable=False),
    sa.Column('action', mysql.TEXT(), nullable=False),
    sa.ForeignKeyConstraint(['bundle_uuid'], ['bundle.uuid'], name='bundle_action_ibfk_1'),
    sa.PrimaryKeyConstraint('id'),
    mysql_default_charset='latin1',
    mysql_engine='InnoDB'
    )

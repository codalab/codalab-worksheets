"""remove data hash

Revision ID: db3ca94867b3
Revises: 22b6c41cd29c
Create Date: 2023-02-15 01:43:30.027124

"""

# revision identifiers, used by Alembic.
revision = 'db3ca94867b3'
down_revision = '22b6c41cd29c'

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('bundle_data_hash_index', table_name='bundle')
    op.drop_column('bundle', 'data_hash')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('bundle', sa.Column('data_hash', mysql.VARCHAR(length=63), nullable=True))
    op.create_index('bundle_data_hash_index', 'bundle', ['data_hash'], unique=False)
    # ### end Alembic commands ###

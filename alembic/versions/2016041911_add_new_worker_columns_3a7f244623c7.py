"""Add new worker columns.

Revision ID: 3a7f244623c7
Revises: 42d0efeae48e
Create Date: 2016-04-19 11:13:34.575143

"""

# revision identifiers, used by Alembic.
revision = '3a7f244623c7'
down_revision = '42d0efeae48e'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('worker', sa.Column('cpus', sa.Integer(), nullable=False))
    op.add_column('worker', sa.Column('memory_bytes', sa.BigInteger(), nullable=False))
    op.add_column('worker', sa.Column('tag', sa.Text(), nullable=True))


def downgrade():
    op.drop_column('worker', 'tag')
    op.drop_column('worker', 'memory_bytes')
    op.drop_column('worker', 'cpus')

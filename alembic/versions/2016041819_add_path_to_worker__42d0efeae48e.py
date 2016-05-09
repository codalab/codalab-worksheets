"""Add path to worker dependencies.

Revision ID: 42d0efeae48e
Revises: 1c2c255e6a01
Create Date: 2016-04-18 19:48:19.931013

"""

# revision identifiers, used by Alembic.
revision = '42d0efeae48e'
down_revision = '1c2c255e6a01'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('worker_dependency', sa.Column('dependency_path', sa.Text(), nullable=False))


def downgrade():
    op.drop_column('worker_dependency', 'dependency_path')

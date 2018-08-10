"""Add anonymous flags

Revision ID: 4ea0403733dc
Revises: 540856fade99
Create Date: 2016-12-05 14:04:39.239593

"""

# revision identifiers, used by Alembic.
revision = '4ea0403733dc'
down_revision = '46d007c5ebc3'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('bundle', sa.Column('is_anonymous', sa.Boolean(), nullable=False))
    op.add_column('worksheet', sa.Column('is_anonymous', sa.Boolean(), nullable=False))


def downgrade():
    op.drop_column('worksheet', 'is_anonymous')
    op.drop_column('bundle', 'is_anonymous')

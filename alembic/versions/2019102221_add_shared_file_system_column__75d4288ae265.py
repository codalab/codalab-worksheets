"""Add shared-file-system column to workers

Revision ID: 75d4288ae265
Revises: d0dd45f443b6
Create Date: 2019-10-22 21:05:26.580918

"""

# revision identifiers, used by Alembic.
revision = '75d4288ae265'
down_revision = 'd0dd45f443b6'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('worker', sa.Column('shared_file_system', sa.Boolean(), nullable=False, server_default='0'))


def downgrade():
    op.drop_column('worker', 'shared_file_system')

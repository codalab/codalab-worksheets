"""Add columns cpuset and gpuset to worker_run table

Revision ID: 19b1e7f3608b
Revises: 3f422c89878d
Create Date: 2018-02-20 07:07:07.633737

"""

# revision identifiers, used by Alembic.
revision = '19b1e7f3608b'
down_revision = '3f422c89878d'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('worker_run', sa.Column('cpuset', sa.LargeBinary(), nullable=False, default=''))
    op.add_column('worker_run', sa.Column('gpuset', sa.LargeBinary(), nullable=False, default=''))


def downgrade():
    op.drop_column('worker_run', 'cpuset')
    op.drop_column('worker_run', 'gpuset')

"""Add terminating to worker

Revision ID: 664d15d50318
Revises: 54e95ff2e718
Create Date: 2020-06-18 22:55:43.411026

"""

# revision identifiers, used by Alembic.
revision = '664d15d50318'
down_revision = '54e95ff2e718'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('worker', sa.Column('is_terminating', sa.Boolean(), nullable=False, server_default=sa.false()))


def downgrade():
    op.drop_column('worker', 'is_terminating')
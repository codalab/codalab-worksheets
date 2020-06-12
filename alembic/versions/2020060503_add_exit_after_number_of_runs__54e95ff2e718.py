"""Add exit after number of runs to worker

Revision ID: 54e95ff2e718
Revises: 9d0f1ffb18e9
Create Date: 2020-06-05 03:44:06.240417

"""

# revision identifiers, used by Alembic.
revision = '54e95ff2e718'
down_revision = '9d0f1ffb18e9'

from alembic import op
import sqlalchemy as sa
import sys

def upgrade():
    op.add_column('worker', sa.Column('exit_after_num_runs', sa.Integer(), nullable=False, server_default=sys.maxsize))


def downgrade():
    op.drop_column('worker', 'exit_after_num_runs')
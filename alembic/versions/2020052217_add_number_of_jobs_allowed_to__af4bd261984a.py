"""Add number of jobs allowed to the worker table

Revision ID: af4bd261984a
Revises: 9d0f1ffb18e9
Create Date: 2020-05-22 17:17:41.798213

"""

# revision identifiers, used by Alembic.
revision = 'af4bd261984a'
down_revision = '9d0f1ffb18e9'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('worker', sa.Column('exit_number_jobs', sa.Integer(), nullable=False))


def downgrade():
    op.drop_column('worker', 'exit_number_jobs')

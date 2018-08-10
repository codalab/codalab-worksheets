"""Add worker tables

Revision ID: 58ce22fe731e
Revises: 40d61632fd13
Create Date: 2016-03-14 20:56:10.411474

"""

# revision identifiers, used by Alembic.
revision = '58ce22fe731e'
down_revision = '40d61632fd13'

from alembic import op


def upgrade():
    # worker tables automatically added.
    pass


def downgrade():
    op.drop_table('worker_socket')
    op.drop_table('worker_run')
    op.drop_table('worker_dependency')
    op.drop_table('worker')

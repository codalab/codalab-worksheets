"""Add index by bundle state.

Revision ID: 26cf8de18f09
Revises: 3a7f244623c7
Create Date: 2016-04-20 11:06:23.054640

"""

# revision identifiers, used by Alembic.
revision = '26cf8de18f09'
down_revision = '3a7f244623c7'

from alembic import op


def upgrade():
    op.create_index('state_index', 'bundle', ['state'], unique=False)


def downgrade():
    op.drop_index('state_index', table_name='bundle')

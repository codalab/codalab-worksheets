"""create bundle permissions table

Revision ID: eb10bb49c6f
Revises: 143201389156
Create Date: 2015-03-27 18:37:05.706591

"""

# revision identifiers, used by Alembic.
revision = 'eb10bb49c6f'
down_revision = '143201389156'

from alembic import op
import sqlalchemy as sa

def upgrade():
    # group_bundle_permission automatically added
    pass

def downgrade():
    op.drop_table('group_bundle_permission')

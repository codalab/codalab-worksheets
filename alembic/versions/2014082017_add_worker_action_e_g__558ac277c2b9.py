"""add worker_action (e.g., supports kill)

Revision ID: 558ac277c2b9
Revises: 341ee10697f1
Create Date: 2014-08-20 17:30:02.374550

"""

# revision identifiers, used by Alembic.
revision = '558ac277c2b9'
down_revision = '341ee10697f1'

from alembic import op
import sqlalchemy as sa


def upgrade():
    pass
    #op.create_table('bundle_action',
    #    sa.Column('id', sa.Integer()),
    #    sa.Column('bundle_uuid', sa.String(length=63), nullable=False),
    #    sa.Column('action', sa.String(length=63), nullable=False),
    #    sa.PrimaryKeyConstraint('id')
    #)

def downgrade():
    op.drop_table('bundle_action')

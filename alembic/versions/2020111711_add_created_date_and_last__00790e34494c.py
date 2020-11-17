"""add created date and last_modified date to worksheet

Revision ID: 00790e34494c
Revises: 4bac4855e710
Create Date: 2020-11-17 11:19:44.380041

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '00790e34494c'
down_revision = '4bac4855e710'


def upgrade():
    op.add_column('worksheet', sa.Column('date_created', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False))
    op.add_column('worksheet', sa.Column('date_last_modified', sa.TIMESTAMP(), nullable=True))


def downgrade():
    op.drop_column('worksheet', 'date_created')
    op.drop_column('worksheet', 'date_last_modified')

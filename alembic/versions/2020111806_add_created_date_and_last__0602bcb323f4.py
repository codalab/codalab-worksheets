"""add created date and last_modified date to worksheet

Revision ID: 0602bcb323f4
Revises: 4bac4855e710
Create Date: 2020-11-18 06:13:52.256466

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0602bcb323f4'
down_revision = '4bac4855e710'


def upgrade():
    op.add_column('worksheet', sa.Column('date_created', sa.DateTime(), nullable=False))
    op.add_column('worksheet', sa.Column('date_last_modified', sa.DateTime(), nullable=True))


def downgrade():
    op.drop_column('worksheet', 'date_last_modified')
    op.drop_column('worksheet', 'date_created')

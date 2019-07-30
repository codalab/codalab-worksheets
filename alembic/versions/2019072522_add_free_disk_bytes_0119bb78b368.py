"""add free disk bytes

Revision ID: 0119bb78b368
Revises: d24522e04593
Create Date: 2019-07-25 22:57:37.611894

"""

# revision identifiers, used by Alembic.
revision = '0119bb78b368'
down_revision = 'd24522e04593'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('worker', sa.Column('free_disk_bytes', sa.BigInteger(), nullable=True))


def downgrade():
    op.drop_column('worker', 'free_disk_bytes')

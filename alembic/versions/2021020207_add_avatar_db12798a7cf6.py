"""add avatar

Revision ID: db12798a7cf6
Revises: ddd9989327c7
Create Date: 2021-02-02 07:49:42.263781

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'db12798a7cf6'
down_revision = 'ddd9989327c7'


def upgrade():
    op.add_column('user', sa.Column('avatar_id', sa.String(length=63), nullable=True))


def downgrade():
    op.drop_column('user', 'avatar_id')

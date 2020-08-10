"""Added has_access to user

Revision ID: c7d985494b8f
Revises: 664d15d50318
Create Date: 2020-07-09 06:40:06.199786

"""

# revision identifiers, used by Alembic.
revision = 'c7d985494b8f'
down_revision = '664d15d50318'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('user', sa.Column('has_access', sa.Boolean(), nullable=True))


def downgrade():
    op.drop_column('user', 'has_access')

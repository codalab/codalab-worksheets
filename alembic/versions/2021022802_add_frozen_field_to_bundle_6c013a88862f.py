"""Add frozen field to bundle.

Revision ID: 6c013a88862f
Revises: e24a53ae9241
Create Date: 2021-02-28 02:31:07.648416

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '6c013a88862f'
down_revision = 'e24a53ae9241'


def upgrade():
    op.add_column('bundle', sa.Column('frozen', sa.DateTime(), nullable=True))


def downgrade():
    op.drop_column('bundle', 'frozen')

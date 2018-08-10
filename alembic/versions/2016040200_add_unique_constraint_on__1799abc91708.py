"""add unique constraint on username and email

Revision ID: 1799abc91708
Revises: 58ce22fe731e
Create Date: 2016-04-02 00:32:19.166417

"""

# revision identifiers, used by Alembic.
revision = '1799abc91708'
down_revision = '58ce22fe731e'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_unique_constraint('uix_2', 'user', ['email'])
    op.create_unique_constraint('uix_3', 'user', ['user_name'])


def downgrade():
    op.drop_constraint('uix_2', 'user', type_='unique')
    op.drop_constraint('uix_3', 'user', type_='unique')

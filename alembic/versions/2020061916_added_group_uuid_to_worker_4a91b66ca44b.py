"""Added group_uuid to worker

Revision ID: 4a91b66ca44b
Revises: 54e95ff2e718
Create Date: 2020-06-19 16:12:06.552677

"""

# revision identifiers, used by Alembic.
revision = '4a91b66ca44b'
down_revision = '54e95ff2e718'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('worker', sa.Column('group_uuid', sa.String(length=63), nullable=True))
    op.create_foreign_key(None, 'worker', 'group', ['group_uuid'], ['uuid'])


def downgrade():
    op.drop_constraint(None, 'worker', type_='foreignkey')
    op.drop_column('worker', 'group_uuid')

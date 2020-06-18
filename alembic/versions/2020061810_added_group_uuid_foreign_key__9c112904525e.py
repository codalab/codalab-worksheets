"""Added group_uuid foreign key to worker

Revision ID: 9c112904525e
Revises: 54e95ff2e718
Create Date: 2020-06-18 10:05:10.071454

"""

# revision identifiers, used by Alembic.
revision = '9c112904525e'
down_revision = '54e95ff2e718'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('worker', sa.Column('group_uuid', sa.String(length=63), nullable=True))
    op.create_foreign_key(None, 'worker', 'group', ['group_uuid'], ['uuid'])


def downgrade():
    op.drop_constraint(None, 'worker', type_='foreignkey')
    op.drop_column('worker', 'group_uuid')

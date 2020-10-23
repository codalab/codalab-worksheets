"""Added group_uuid to worker

Revision ID: 9d3ab0b9d304
Revises: c7d985494b8f
Create Date: 2020-08-10 18:07:31.646054

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '9d3ab0b9d304'
down_revision = 'c7d985494b8f'


def upgrade():
    op.add_column('worker', sa.Column('group_uuid', sa.String(length=63), nullable=True))
    op.create_foreign_key(None, 'worker', 'group', ['group_uuid'], ['uuid'])


def downgrade():
    op.drop_constraint(None, 'worker', type_='foreignkey')
    op.drop_column('worker', 'group_uuid')

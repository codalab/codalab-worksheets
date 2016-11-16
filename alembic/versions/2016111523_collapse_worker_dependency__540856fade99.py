"""Collapse worker_dependency rows

Revision ID: 540856fade99
Revises: 309cf9c796b9
Create Date: 2016-11-15 23:51:29.445143

"""

# revision identifiers, used by Alembic.
revision = '540856fade99'
down_revision = '309cf9c796b9'

from alembic import op
import sqlalchemy as sa


def upgrade():
    # Add active flag to worker
    op.add_column('worker', sa.Column('is_active', sa.Boolean(), nullable=False))

    # Use worker_dependency as a key-value store, with serialized dependencies in the MEDIUMBLOB
    op.add_column('worker_dependency', sa.Column('dependencies', sa.LargeBinary(), nullable=False))
    op.drop_column('worker_dependency', 'dependency_uuid')
    op.drop_column('worker_dependency', 'dependency_path')

    # Each user/worker will only have one row now
    op.execute("ALTER TABLE `worker_dependency` ADD PRIMARY KEY (`user_id`, `worker_id`);")


def downgrade():
    # Need to temporarily drop foreign keys to drop the primary key constraint
    op.drop_constraint('worker_dependency_ibfk_1', 'worker_dependency', type_='foreignkey')
    op.drop_constraint('worker_dependency_ibfk_2', 'worker_dependency', type_='foreignkey')
    op.execute("ALTER TABLE `worker_dependency` DROP PRIMARY KEY;")
    op.create_foreign_key('worker_dependency_ibfk_1', 'worker_dependency', 'worker', ['user_id', 'worker_id'], ['user_id', 'worker_id'])
    op.create_foreign_key('worker_dependency_ibfk_2', 'worker_dependency', 'user', ['user_id'], ['user_id'])

    # Add back old columns, though we can't get back old data. But it's okay, the data is transient anyway.
    op.add_column('worker_dependency', sa.Column('dependency_path', sa.Text(), nullable=False))
    op.add_column('worker_dependency', sa.Column('dependency_uuid', sa.String(63), nullable=False))

    # Remove new columns
    op.drop_column('worker_dependency', 'dependencies')
    op.drop_column('worker', 'is_active')

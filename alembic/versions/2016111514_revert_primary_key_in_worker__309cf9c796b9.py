"""Revert primary key in worker_dependency
Inverse of the parent revision

Revision ID: 309cf9c796b9
Revises: 54725ffc62f3
Create Date: 2016-11-15 14:42:44.198134

"""

# revision identifiers, used by Alembic.
revision = '309cf9c796b9'
down_revision = '54725ffc62f3'

from alembic import op


def upgrade():
    op.drop_column('worker_dependency', 'id')


def downgrade():
    # Cannot add primary key with auto-increment natively in alembic
    # Note that this is MySQL-specific
    op.execute("ALTER TABLE `worker_dependency` ADD `id` INT PRIMARY KEY AUTO_INCREMENT FIRST;")


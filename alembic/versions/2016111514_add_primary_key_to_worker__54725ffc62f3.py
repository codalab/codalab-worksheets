"""Add primary key to worker_dependency

Revision ID: 54725ffc62f3
Revises: 730e212b938
Create Date: 2016-11-15 14:02:41.621934

"""

# revision identifiers, used by Alembic.
revision = '54725ffc62f3'
down_revision = '730e212b938'

from alembic import op


def upgrade():
    # Cannot add primary key with auto-increment natively in alembic
    # Note that this is MySQL-specific
    op.execute("ALTER TABLE `worker_dependency` ADD `id` INT PRIMARY KEY AUTO_INCREMENT FIRST;")


def downgrade():
    op.drop_column('worker_dependency', 'id')

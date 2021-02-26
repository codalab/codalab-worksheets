"""azure bundle storage type columns

Revision ID: e24a53ae9241
Revises: db12798a7cf6
Create Date: 2021-02-24 19:46:03.751097

"""

# revision identifiers, used by Alembic.
revision = 'e24a53ae9241'
down_revision = 'db12798a7cf6'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('bundle', sa.Column('storage_type', sa.Enum("disk", "azure_blob"), nullable=True))
    op.execute("UPDATE bundle set storage_type = 'disk'")
    op.add_column('bundle', sa.Column('is_dir', sa.Boolean, nullable=True))


def downgrade():
    op.drop_column('bundle', 'storage_type')
    op.drop_column('bundle', 'is_dir')

"""Remove unused metadata.

Revision ID: 730e212b938
Revises: 4bc4499383aa
Create Date: 2016-06-06 15:21:12.509623

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = '730e212b938'
down_revision = '4bc4499383aa'


def upgrade():
    op.execute('DELETE FROM bundle_metadata WHERE metadata_key IN ("temp_dir", "disk_read", "disk_write")')


def downgrade():
    # No going back.
    pass

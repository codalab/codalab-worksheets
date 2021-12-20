"""bundle store name unique

Revision ID: 685cb4bc0b79
Revises: 26a5e6b3bfa5
Create Date: 2021-12-12 17:20:58.853560

"""

# revision identifiers, used by Alembic.
revision = '685cb4bc0b79'
down_revision = '26a5e6b3bfa5'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_unique_constraint('nix_1', 'bundle_store', ['name'])


def downgrade():
    op.drop_constraint('nix_1', 'bundle_store', type_='unique')

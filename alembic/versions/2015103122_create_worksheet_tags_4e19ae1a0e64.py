"""create worksheet tags

Revision ID: 4e19ae1a0e64
Revises: 12a9451988cd
Create Date: 2015-10-31 22:39:27.688995

"""

# revision identifiers, used by Alembic.
revision = '4e19ae1a0e64'
down_revision = '12a9451988cd'

from alembic import op
import sqlalchemy as sa


def upgrade():
    # worksheet_tag automatically added
    pass

def downgrade():
    op.drop_table('worksheet_tag')

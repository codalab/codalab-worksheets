"""Add owner_id to bundle

Revision ID: 341ee10697f1
Revises: None
Create Date: 2014-08-06 13:55:00.967016

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '341ee10697f1'
down_revision = None


def upgrade():
    # commands auto generated by Alembic - please adjust! #
    op.add_column('bundle', sa.Column('owner_id', sa.Integer(), nullable=True))
    # end Alembic commands #


def downgrade():
    # commands auto generated by Alembic - please adjust! #
    op.drop_column('bundle', 'owner_id')
    # end Alembic commands #

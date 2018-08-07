"""add email notifications

Revision ID: 37e06c8c5e5d
Revises: 540856fade99
Create Date: 2017-01-16 08:08:20.494140

"""

# revision identifiers, used by Alembic.
revision = "37e06c8c5e5d"
down_revision = "540856fade99"

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column("user", sa.Column("notifications", sa.Integer(), nullable=False))
    conn = op.get_bind()
    conn.execute("UPDATE user SET notifications = 2")


def downgrade():
    op.drop_column("user", "notifications")

"""Augment user table and add oauth tables

Revision ID: 58eccccb346d
Revises: 5aea7b8ff415
Create Date: 2016-02-04 01:45:33.237016

"""

# revision identifiers, used by Alembic.
revision = '58eccccb346d'
down_revision = '5aea7b8ff415'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('user', sa.Column('date_joined', sa.DateTime(), nullable=False))
    op.add_column('user', sa.Column('email', sa.String(length=254), nullable=False))
    op.add_column('user', sa.Column('first_name', sa.String(length=30), nullable=True))
    op.add_column('user', sa.Column('is_active', sa.Boolean(), nullable=False))
    op.add_column('user', sa.Column('is_verified', sa.Boolean(), nullable=False))
    op.add_column('user', sa.Column('is_superuser', sa.Boolean(), nullable=False))
    op.add_column('user', sa.Column('last_login', sa.DateTime(), nullable=True))
    op.add_column('user', sa.Column('last_name', sa.String(length=30), nullable=True))
    op.add_column('user', sa.Column('password', sa.String(length=128), nullable=False))
    op.create_unique_constraint('uix_1', 'user', ['user_id'])
    op.create_foreign_key('fk__user_group__user', 'user_group', 'user', ['user_id'], ['user_id'])

    # create oauth2_access_token, oauth2_auth_code, oauth2_refresh_token, oauth2_client
    pass


def downgrade():
    op.drop_constraint('fk__user_group__user', 'user_group', type_='foreignkey')
    op.drop_constraint('uix_1', 'user', type_='unique')
    op.drop_column('user', 'password')
    op.drop_column('user', 'last_name')
    op.drop_column('user', 'last_login')
    op.drop_column('user', 'is_superuser')
    op.drop_column('user', 'is_verified')
    op.drop_column('user', 'is_active')
    op.drop_column('user', 'first_name')
    op.drop_column('user', 'email')
    op.drop_column('user', 'date_joined')
    op.drop_table('oauth2_access_token')
    op.drop_table('oauth2_auth_code')
    op.drop_table('oauth2_refresh_token')
    op.drop_table('oauth2_client')

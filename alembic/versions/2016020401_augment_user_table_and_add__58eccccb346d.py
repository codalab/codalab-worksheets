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
    op.add_column('user', sa.Column('affiliation', sa.String(255), nullable=True))
    op.add_column('user', sa.Column('url', sa.String(255), nullable=True))
    op.create_unique_constraint('uix_1', 'user', ['user_id'])
    op.create_foreign_key('fk__user_group__user', 'user_group', 'user', ['user_id'], ['user_id'])

    # Necessary since we normally call "dbmetadata.create_all()" first
    op.execute("DROP TABLE IF EXISTS oauth2_token")
    op.execute("DROP TABLE IF EXISTS oauth2_auth_code")
    op.execute("DROP TABLE IF EXISTS oauth2_client")
    op.execute("DROP TABLE IF EXISTS user_verification")

    op.create_table(
        'user_verification',
        sa.Column('id', sa.Integer, primary_key=True, nullable=False),
        sa.Column('user_id', sa.String(63), sa.ForeignKey("user.user_id"), nullable=False),
        sa.Column('date_created', sa.DateTime, nullable=False),
        sa.Column('date_sent', sa.DateTime, nullable=True),
        sa.Column('key', sa.String(64), nullable=False),
        sqlite_autoincrement=True,
    )

    op.create_table(
        'oauth2_client',
        sa.Column('id', sa.Integer, primary_key=True, nullable=False),
        sa.Column('client_id', sa.String(63), nullable=False),
        sa.Column('name', sa.String(63), nullable=True),
        sa.Column('secret', sa.String(255), nullable=True),
        sa.Column('user_id', sa.String(63), sa.ForeignKey("user.user_id"), nullable=True),
        sa.Column('grant_type', sa.Enum("authorization_code", "password", "client_credentials", "refresh_token"), nullable=False),
        sa.Column('response_type', sa.Enum("code", "token"), nullable=False),
        sa.Column('scopes', sa.Text, nullable=False),  # comma-separated list of allowed scopes
        sa.Column('redirect_uris', sa.Text, nullable=False),  # comma-separated list of allowed redirect URIs
        sqlite_autoincrement=True,
    )
    op.create_unique_constraint('uix_1', 'oauth2_client', ['client_id'])

    op.create_table(
        'oauth2_token',
        sa.Column('id', sa.Integer, primary_key=True, nullable=False),
        sa.Column('client_id', sa.String(63), sa.ForeignKey("oauth2_client.client_id"), nullable=False),
        sa.Column('user_id', sa.String(63), sa.ForeignKey("user.user_id"), nullable=False),
        sa.Column('scopes', sa.Text, nullable=False),
        sa.Column('access_token', sa.String(255), unique=True),
        sa.Column('refresh_token', sa.String(255), unique=True),
        sa.Column('expires', sa.DateTime, nullable=False),
        sqlite_autoincrement=True,
    )

    op.create_table(
        'oauth2_auth_code',
        sa.Column('id', sa.Integer, primary_key=True, nullable=False),
        sa.Column('client_id', sa.String(63), sa.ForeignKey("oauth2_client.client_id"), nullable=False),
        sa.Column('user_id', sa.String(63), sa.ForeignKey("user.user_id"), nullable=False),
        sa.Column('scopes', sa.Text, nullable=False),
        sa.Column('code', sa.String(100), nullable=False),
        sa.Column('expires', sa.DateTime, nullable=False),
        sa.Column('redirect_uri', sa.String(255), nullable=False),
        sqlite_autoincrement=True,
    )


def downgrade():
    op.drop_constraint('fk__user_group__user', 'user_group', type_='foreignkey')
    op.drop_constraint('uix_1', 'user', type_='unique')
    op.drop_column('user', 'url')
    op.drop_column('user', 'affiliation')
    op.drop_column('user', 'password')
    op.drop_column('user', 'last_name')
    op.drop_column('user', 'last_login')
    op.drop_column('user', 'is_superuser')
    op.drop_column('user', 'is_verified')
    op.drop_column('user', 'is_active')
    op.drop_column('user', 'first_name')
    op.drop_column('user', 'email')
    op.drop_column('user', 'date_joined')
    op.drop_table('oauth2_token')
    op.drop_table('oauth2_auth_code')
    op.drop_constraint('uix_1', 'oauth2_client', type_='unique')
    op.drop_table('oauth2_client')
    op.drop_table('user_verification')

"""Explicitly set autoincrement to True

Revision ID: 4bac4855e710
Revises: fcb22a612d2a
Create Date: 2020-11-05 10:27:43.989896

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '4bac4855e710'
down_revision = 'fcb22a612d2a'


def upgrade():
    op.alter_column(
        "bundle",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "bundle_metadata",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "bundle_dependency",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "worksheet",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "worksheet_item",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "worksheet_tag",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "group",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "user_group",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "group_bundle_permission",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "group_object_permission",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "user",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "user_verification",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "user_reset_code",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "oauth2_client",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "oauth2_token",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "oauth2_auth_code",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "chat",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )


def downgrade():
    op.alter_column(
        "bundle",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "bundle_metadata",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "bundle_dependency",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "worksheet",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "worksheet_item",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "worksheet_tag",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "group",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "user_group",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "group_bundle_permission",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "group_object_permission",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "user",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "user_verification",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "user_reset_code",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "oauth2_client",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "oauth2_token",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "oauth2_auth_code",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )
    op.alter_column(
        "chat",
        'id',
        type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        nullable=False,
        autoincrement=True,
    )

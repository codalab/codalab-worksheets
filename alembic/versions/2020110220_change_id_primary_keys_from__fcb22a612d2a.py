"""Change id primary keys from Integer to BigInteger, retaining auto-increment

Revision ID: fcb22a612d2a
Revises: 9d3ab0b9d304
Create Date: 2020-11-02 20:57:08.361251

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'fcb22a612d2a'
down_revision = '9d3ab0b9d304'


def upgrade():
    op.alter_column("bundle", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("bundle_metadata", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("bundle_dependency", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("worksheet", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("worksheet_item", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("worksheet_tag", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("group", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("user_group", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("group_bundle_permission", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("group_object_permission", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("user", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("user_verification", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("user_reset_code", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("oauth2_client", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("oauth2_token", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("oauth2_auth_code", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)
    op.alter_column("chat", 'id', type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"), existing_type=sa.Integer(), nullable=False)


def downgrade():
    op.alter_column("bundle", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("bundle_metadata", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("bundle_dependency", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("worksheet", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("worksheet_item", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("worksheet_tag", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("group", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("user_group", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("group_bundle_permission", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("group_object_permission", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("user", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("user_verification", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("user_reset_code", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("oauth2_client", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("oauth2_token", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("oauth2_auth_code", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)
    op.alter_column("chat", 'id', type_=sa.Integer(), existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"), nullable=False)

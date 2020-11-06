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
TABLES = [
    "bundle",
    "bundle_metadata",
    "bundle_dependency",
    "worksheet",
    "worksheet_item",
    "worksheet_tag",
    "group",
    "user_group",
    "group_bundle_permission",
    "group_object_permission",
    "user",
    "user_verification",
    "user_reset_code",
    "oauth2_client",
    "oauth2_token",
    "oauth2_auth_code",
    "chat",
]


def upgrade():
    for table in TABLES:
        # This is necessary because the previous revision fcb22a612d2a suffers from a bug.
        # In case (1), when upgrading to fcb22a612d2a , the id column does not retain the auto_increment property.
        # In case (2), when starting from fcb22a612d2a , the id column has the auto_increment property.
        # In case (1), upgrading to this revision, alembic is unable to make the id column auto_incrementing
        # if it has a row with the id 0. The error is something like:
        # ERROR 1062 (23000): ALTER TABLE causes auto_increment resequencing, resulting in duplicate entry
        # '1' for key 'PRIMARY'
        #
        # This happens because the AUTO_INCREMENT value starts at 1, so a column with AUTO_INCREMENT
        # cannot have a 0 row. As a result, we get around this by moving the 0 to the end of the table (the next ID),
        # before setting it to be auto-incrementing.
        # Note that this is MySQL-specific.
        op.execute(
            f'''
        SET @maxid = (SELECT MAX(id)+1 FROM `{table}`);
        UPDATE `{table}` SET id = @maxid WHERE id = 0;
        '''
        )
        op.alter_column(
            table,
            'id',
            type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
            existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
            nullable=False,
            autoincrement=True,
        )


def downgrade():
    for table in TABLES:
        op.alter_column(
            table,
            'id',
            type_=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
            existing_type=sa.BigInteger().with_variant(sa.Integer, "sqlite"),
            nullable=False,
            autoincrement=True,
        )

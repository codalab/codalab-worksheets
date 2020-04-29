"""remove event table

Revision ID: d0dd45f443b6
Revises: 0119bb78b368
Create Date: 2019-09-09 11:38:31.284742

"""

# revision identifiers, used by Alembic.
revision = 'd0dd45f443b6'
down_revision = '0119bb78b368'

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

def upgrade():
    op.drop_index('events_command_index', table_name='event')
    op.drop_index('events_date_index', table_name='event')
    op.drop_index('events_user_id_index', table_name='event')
    op.drop_index('events_user_name_index', table_name='event')
    op.drop_index('events_uuid_index', table_name='event')
    op.drop_table('event')


def downgrade():
    op.create_table('event',
    sa.Column('id', mysql.INTEGER(display_width=11), nullable=False),
    sa.Column('date', mysql.VARCHAR(length=63), nullable=False),
    sa.Column('start_time', mysql.DATETIME(), nullable=False),
    sa.Column('end_time', mysql.DATETIME(), nullable=False),
    sa.Column('duration', mysql.FLOAT(), nullable=False),
    sa.Column('user_id', mysql.VARCHAR(length=63), nullable=True),
    sa.Column('user_name', mysql.VARCHAR(length=63), nullable=True),
    sa.Column('command', mysql.VARCHAR(length=63), nullable=False),
    sa.Column('args', mysql.TEXT(), nullable=False),
    sa.Column('uuid', mysql.VARCHAR(length=63), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    mysql_default_charset='latin1',
    mysql_engine='InnoDB'
    )
    op.create_index('events_uuid_index', 'event', ['uuid'], unique=False)
    op.create_index('events_user_name_index', 'event', ['user_name'], unique=False)
    op.create_index('events_user_id_index', 'event', ['user_id'], unique=False)
    op.create_index('events_date_index', 'event', ['date'], unique=False)
    op.create_index('events_command_index', 'event', ['command'], unique=False)

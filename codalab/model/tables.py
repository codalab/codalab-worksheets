'''
The SQLAlchemy table objects for the CodaLab bundle system tables.
'''
from sqlalchemy import (
  Column,
  ForeignKey,
  Index,
  MetaData,
  Table,
  UniqueConstraint,
)
from sqlalchemy.types import (
  Integer,
  String,
  Text,
)


db_metadata = MetaData()

bundle = Table(
  'bundle',
  db_metadata,
  Column('id', Integer, primary_key=True, nullable=False),
  Column('uuid', String(63), nullable=False),
  Column('bundle_type', String(63), nullable=False),
  # The command will be NULL except for run bundles.
  Column('command', Text, nullable=True),
  # The data_hash will be NULL if the bundle's value is still being computed.
  Column('data_hash', String(63), nullable=True),
  Column('state', String(63), nullable=False),
  UniqueConstraint('uuid', name='uix_1'),
  Index('bundle_data_hash_index', 'data_hash'),
  sqlite_autoincrement=True,
)

bundle_metadata = Table(
  'bundle_metadata',
  db_metadata,
  Column('id', Integer, primary_key=True, nullable=False),
  Column('bundle_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=False),
  Column('metadata_key', String(63), nullable=False),
  Column('metadata_value', Text, nullable=False),
  Index('metadata_kv_index', 'metadata_key', 'metadata_value', mysql_length=255),
  sqlite_autoincrement=True,
)

dependency = Table(
  'dependency',
  db_metadata,
  Column('id', Integer, primary_key=True, nullable=False),
  Column('child_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=False),
  Column('child_path', Text, nullable=False),
  Column('parent_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=False),
  Column('parent_path', Text, nullable=False),
  sqlite_autoincrement=True,
)

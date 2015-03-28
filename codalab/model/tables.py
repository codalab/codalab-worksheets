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
  Boolean,
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
  Column('owner_id', String(255), nullable=True),
  UniqueConstraint('uuid', name='uix_1'),
  Index('bundle_data_hash_index', 'data_hash'),
  sqlite_autoincrement=True,
)

# Includes things like name, description, etc.
bundle_metadata = Table(
  'bundle_metadata',
  db_metadata,
  Column('id', Integer, primary_key=True, nullable=False),
  Column('bundle_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=False),
  Column('metadata_key', String(63), nullable=False),
  Column('metadata_value', Text, nullable=False),
  Index('metadata_kv_index', 'metadata_key', 'metadata_value', mysql_length=63),
  sqlite_autoincrement=True,
)

# For each child_uuid, we have: key = child_path, target = (parent_uuid, parent_path)
bundle_dependency = Table(
  'bundle_dependency',
  db_metadata,
  Column('id', Integer, primary_key=True, nullable=False),
  Column('child_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=False),
  Column('child_path', Text, nullable=False),
  Column('parent_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=False),
  Column('parent_path', Text, nullable=False),
  sqlite_autoincrement=True,
)

# Stores actions sent from the client to the worker.
bundle_action = Table(
  'bundle_action',
  db_metadata,
  Column('id', Integer, primary_key=True, nullable=False),
  Column('bundle_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=False),
  Column('action', String(63), nullable=False),
  sqlite_autoincrement=True,
)

# The worksheet table does not have many columns now, but it will eventually
# include columns for owner, group, permissions, etc.
worksheet = Table(
  'worksheet',
  db_metadata,
  Column('id', Integer, primary_key=True, nullable=False),
  Column('uuid', String(63), nullable=False),
  Column('name', String(255), nullable=False),
  Column('owner_id', String(255), nullable=True),
  UniqueConstraint('uuid', name='uix_1'),
  Index('worksheet_name_index', 'name'),
  Index('worksheet_owner_index', 'owner_id'),
  sqlite_autoincrement=True,
)

worksheet_item = Table(
  'worksheet_item',
  db_metadata,
  Column('id', Integer, primary_key=True, nullable=False),
  Column('worksheet_uuid', String(63), ForeignKey(worksheet.c.uuid), nullable=False),

  # A worksheet item is either:
  # - type = bundle (bundle_uuid != null)
  # - type = worksheet (subworksheet_uuid != null)
  # - type = markup (value != null)
  # - type = directive (value != null)
  Column('bundle_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=True),
  Column('subworksheet_uuid', String(63), ForeignKey(worksheet.c.uuid), nullable=True),
  Column('value', Text, nullable=False),  # TODO: make this nullable
  Column('type', String(20), nullable=False),

  Column('sort_key', Integer, nullable=True),
  Index('worksheet_item_worksheet_uuid_index', 'worksheet_uuid'),
  Index('worksheet_item_bundle_uuid_index', 'bundle_uuid'),
  Index('worksheet_item_subworksheet_uuid_index', 'subworksheet_uuid'),
  sqlite_autoincrement=True,
)

group = Table(
  'group',
  db_metadata,
  Column('id', Integer, primary_key=True, nullable=False),
  Column('uuid', String(63), nullable=False),
  Column('name', String(255), nullable=False),
  Column('user_defined', Boolean),
  Column('owner_id', String(255), nullable=True),
  UniqueConstraint('uuid', name='uix_1'),
  Index('group_name_index', 'name'),
  Index('group_owner_id_index', 'owner_id'),
  sqlite_autoincrement=True,
)

user_group = Table(
  'user_group',
  db_metadata,
  Column('id', Integer, primary_key=True, nullable=False),
  Column('group_uuid', String(63), ForeignKey(group.c.uuid), nullable=False),
  Column('user_id', String(63), nullable=False),
  # Whether a user is able to modify this group.
  Column('is_admin', Boolean),
  Index('group_uuid_index', 'group_uuid'),
  Index('user_id_index', 'user_id'),
  sqlite_autoincrement=True,
)

# Permissions for bundles
group_bundle_permission = Table(
  'group_bundle_permission',
  db_metadata,
  Column('id', Integer, primary_key=True, nullable=False),
  Column('group_uuid', String(63), ForeignKey(group.c.uuid), nullable=False),
  # Reference to a bundle
  Column('object_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=False),
  # Permissions encoded as integer (see below)
  Column('permission', Integer, nullable=False),
  sqlite_autoincrement=True,
)

# Permissions for worksheets
group_object_permission = Table(
  'group_object_permission',
  db_metadata,
  Column('id', Integer, primary_key=True, nullable=False),
  Column('group_uuid', String(63), ForeignKey(group.c.uuid), nullable=False),
  # Reference to a worksheet object
  Column('object_uuid', String(63), ForeignKey(worksheet.c.uuid), nullable=False),
  # Permissions encoded as integer (see below)
  Column('permission', Integer, nullable=False),
  sqlite_autoincrement=True,
)

# A permission value is one of the following: none (0), read (1), or all (2).
GROUP_OBJECT_PERMISSION_NONE = 0x00
GROUP_OBJECT_PERMISSION_READ = 0x01
GROUP_OBJECT_PERMISSION_ALL = 0x02

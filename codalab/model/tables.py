"""
The SQLAlchemy table objects for the CodaLab bundle system tables.
"""
# TODO: Replace String and Text columns with Unicode and UnicodeText as appropriate
# This way, SQLAlchemy will automatically perform conversions to and from UTF-8
# encoding, or use appropriate database engine-specific data types for Unicode
# data. Currently, only worksheet.title uses the Unicode column type.
from sqlalchemy import Column, ForeignKey, Index, MetaData, Table, UniqueConstraint
from sqlalchemy.types import (
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    Float,
    Integer,
    LargeBinary,
    String,
    Text,
    Unicode,
)
from sqlalchemy.sql.schema import ForeignKeyConstraint

db_metadata = MetaData()

bundle = Table(
    'bundle',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('uuid', String(63), nullable=False),
    Column('bundle_type', String(63), nullable=False),
    # The command will be NULL except for run bundles.
    Column('command', Text, nullable=True),
    # The data_hash will be NULL if the bundle's value is still being computed.
    Column('data_hash', String(63), nullable=True),
    Column('state', String(63), nullable=False),
    Column('owner_id', String(255), nullable=True),
    Column('is_anonymous', Boolean, nullable=False, default=False),
    UniqueConstraint('uuid', name='uix_1'),
    Index('bundle_data_hash_index', 'data_hash'),
    Index('state_index', 'state'),  # Needed for the bundle manager.
)

# Includes things like name, description, etc.
bundle_metadata = Table(
    'bundle_metadata',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('bundle_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=False),
    Column('metadata_key', String(63), nullable=False),
    Column('metadata_value', Text, nullable=False),
    Index('metadata_kv_index', 'metadata_key', 'metadata_value', mysql_length=63),
)

# For each child_uuid, we have: key = child_path, target = (parent_uuid, parent_path)
bundle_dependency = Table(
    'bundle_dependency',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('child_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=False),
    Column('child_path', Text, nullable=False),
    # Deliberately omit ForeignKey(bundle.c.uuid), because bundles can have
    # dependencies to bundles not (yet) in the system.
    Column('parent_uuid', String(63), nullable=False),
    Column('parent_path', Text, nullable=False),
)

# The worksheet table does not have many columns now, but it will eventually
# include columns for owner, group, permissions, etc.
worksheet = Table(
    'worksheet',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('uuid', String(63), nullable=False),
    Column('name', String(255), nullable=False),
    Column('owner_id', String(255), nullable=True),
    Column(
        'title', Unicode(255), nullable=True
    ),  # Short human-readable description of the worksheet
    Column(
        'frozen', DateTime, nullable=True
    ),  # When the worksheet was frozen (forever immutable) if it is.
    Column('is_anonymous', Boolean, nullable=False, default=False),
    Column(
        'date_created', DateTime
    ),  # When the worksheet was created; Set to null if the worksheet created before v0.5.31; Set to current timestamp by default
    Column(
        'date_last_modified', DateTime
    ),  # When the worksheet was last modified; Set to null if the worksheet created before v0.5.31; Set to current_timestamp by default
    UniqueConstraint('uuid', name='uix_1'),
    Index('worksheet_name_index', 'name'),
    Index('worksheet_owner_index', 'owner_id'),
)

worksheet_item = Table(
    'worksheet_item',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('worksheet_uuid', String(63), ForeignKey(worksheet.c.uuid), nullable=False),
    # A worksheet item is either:
    # - type = bundle (bundle_uuid != null)
    # - type = worksheet (subworksheet_uuid != null)
    # - type = markup (value != null)
    # - type = directive (value != null)
    # Deliberately omit ForeignKey(bundle.c.uuid), because worksheets can contain
    # bundles and worksheets not (yet) in the system.
    Column('bundle_uuid', String(63), nullable=True),
    Column('subworksheet_uuid', String(63), nullable=True),
    Column('value', Text, nullable=False),  # TODO: make this nullable
    Column('type', String(20), nullable=False),
    Column('sort_key', Integer, nullable=True),
    Index('worksheet_item_worksheet_uuid_index', 'worksheet_uuid'),
    Index('worksheet_item_bundle_uuid_index', 'bundle_uuid'),
    Index('worksheet_item_subworksheet_uuid_index', 'subworksheet_uuid'),
)

# Worksheet tags
worksheet_tag = Table(
    'worksheet_tag',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('worksheet_uuid', String(63), ForeignKey(worksheet.c.uuid), nullable=False),
    Column('tag', String(63), nullable=False),
    Index('worksheet_tag_worksheet_uuid_index', 'worksheet_uuid'),
    Index('worksheet_tag_tag_index', 'tag'),
)

group = Table(
    'group',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('uuid', String(63), nullable=False),
    Column('name', String(255), nullable=False),
    Column('user_defined', Boolean),
    Column('owner_id', String(255), nullable=True),
    UniqueConstraint('uuid', name='uix_1'),
    Index('group_name_index', 'name'),
    Index('group_owner_id_index', 'owner_id'),
)

user_group = Table(
    'user_group',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('group_uuid', String(63), ForeignKey(group.c.uuid), nullable=False),
    Column('user_id', String(63), ForeignKey("user.user_id"), nullable=False),
    # Whether a user is able to modify this group.
    Column('is_admin', Boolean),
    Index('group_uuid_index', 'group_uuid'),
    Index('user_id_index', 'user_id'),
)

# Permissions for bundles
group_bundle_permission = Table(
    'group_bundle_permission',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('group_uuid', String(63), ForeignKey(group.c.uuid), nullable=False),
    # Reference to a bundle
    Column('object_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=False),
    # Permissions encoded as integer (see below)
    Column('permission', Integer, nullable=False),
)

# Permissions for worksheets
group_object_permission = Table(
    'group_object_permission',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('group_uuid', String(63), ForeignKey(group.c.uuid), nullable=False),
    # Reference to a worksheet object
    Column('object_uuid', String(63), ForeignKey(worksheet.c.uuid), nullable=False),
    # Permissions encoded as integer (see below)
    Column('permission', Integer, nullable=False),
)

# A permission value is one of the following: none (0), read (1), or all (2).
GROUP_OBJECT_PERMISSION_NONE = 0x00
GROUP_OBJECT_PERMISSION_READ = 0x01
GROUP_OBJECT_PERMISSION_ALL = 0x02

# A notifications value is one of the following:
NOTIFICATIONS_NONE = 0x00  # Receive no notifications
NOTIFICATIONS_IMPORTANT = 0x01  # Receive only important notifications
NOTIFICATIONS_GENERAL = 0x02  # Receive general notifications (new features)

# Store information about users.
user = Table(
    'user',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    # Basic information
    Column('user_id', String(63), nullable=False),
    Column('user_name', String(63), nullable=False, unique=True),
    Column(
        'email', String(254), nullable=False, unique=True
    ),  # Length of 254 to be compliant with RFC3696/5321
    Column(
        'notifications', Integer, nullable=False, default=NOTIFICATIONS_GENERAL
    ),  # Which emails user wants to receive
    Column('last_login', DateTime),  # Null if user has never logged in
    Column(
        'is_active', Boolean, nullable=False, default=True
    ),  # Set to False instead of deleting users to maintain foreign key integrity
    Column('first_name', String(30, convert_unicode=True)),
    Column('last_name', String(30, convert_unicode=True)),
    Column('date_joined', DateTime, nullable=False),
    Column('has_access', Boolean, default=False, nullable=True),
    Column('is_verified', Boolean, nullable=False, default=False),
    Column('is_superuser', Boolean, nullable=False, default=False),
    Column('password', String(128), nullable=False),
    # Additional information
    Column('affiliation', String(255, convert_unicode=True), nullable=True),
    Column('url', String(255, convert_unicode=True), nullable=True),
    # Quotas
    Column('time_quota', Float, nullable=False),  # Number of seconds allowed
    Column('parallel_run_quota', Integer, nullable=False),  # Number of parallel jobs allowed
    Column('time_used', Float, nullable=False),  # Number of seconds already used
    Column('disk_quota', Float, nullable=False),  # Number of bytes allowed
    Column('disk_used', Float, nullable=False),  # Number of bytes already used
    Column(
        'avatar_id', String(63), nullable=True
    ),  # bundle id of the user's uploaded profile picture; Null if the user has never uploaded one
    Index('user_user_id_index', 'user_id'),
    Index('user_user_name_index', 'user_name'),
    UniqueConstraint('user_id', name='uix_1'),
)

# Stores (email) verification keys
user_verification = Table(
    'user_verification',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('user_id', String(63), ForeignKey(user.c.user_id), nullable=False),
    Column('date_created', DateTime, nullable=False),
    Column('date_sent', DateTime, nullable=True),
    Column('key', String(64), nullable=False),
)

# Stores password reset codes
user_reset_code = Table(
    'user_reset_code',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('user_id', String(63), ForeignKey(user.c.user_id), nullable=False),
    Column('date_created', DateTime, nullable=False),
    Column('code', String(64), nullable=False),
)

# OAuth2 Tables

oauth2_client = Table(
    'oauth2_client',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('client_id', String(63), nullable=False),
    Column('name', String(63), nullable=True),
    Column('secret', String(255), nullable=True),
    Column('user_id', String(63), ForeignKey(user.c.user_id), nullable=True),
    Column(
        'grant_type',
        Enum("authorization_code", "password", "client_credentials", "refresh_token"),
        nullable=False,
    ),
    Column('response_type', Enum("code", "token"), nullable=False),
    Column('scopes', Text, nullable=False),  # comma-separated list of allowed scopes
    Column('redirect_uris', Text, nullable=False),  # comma-separated list of allowed redirect URIs
    UniqueConstraint('client_id', name='uix_1'),
)

oauth2_token = Table(
    'oauth2_token',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('client_id', String(63), ForeignKey(oauth2_client.c.client_id), nullable=False),
    Column('user_id', String(63), ForeignKey(user.c.user_id), nullable=False),
    Column('scopes', Text, nullable=False),
    Column('access_token', String(255), unique=True),
    Column('refresh_token', String(255), unique=True),
    Column('expires', DateTime, nullable=False),
)

oauth2_auth_code = Table(
    'oauth2_auth_code',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),
    Column('client_id', String(63), ForeignKey(oauth2_client.c.client_id), nullable=False),
    Column('user_id', String(63), ForeignKey(user.c.user_id), nullable=False),
    Column('scopes', Text, nullable=False),
    Column('code', String(100), nullable=False),
    Column('expires', DateTime, nullable=False),
    Column('redirect_uri', String(255), nullable=False),
)

# Store information about users' questions or feedback.
chat = Table(
    'chat',
    db_metadata,
    Column(
        'id',
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    ),  # Primary key
    Column('time', DateTime, nullable=False),  # When did the user send this query?
    Column('sender_user_id', String(63), nullable=True),  # Who sent it?
    Column('recipient_user_id', String(63), nullable=True),  # Who received it?
    Column('message', Text, nullable=False),  # What's the content of the chat?
    Column(
        'worksheet_uuid', String(63), nullable=True
    ),  # What is the id of the worksheet that the sender is on?
    Column(
        'bundle_uuid', String(63), nullable=True
    ),  # What is the id of the bundle that the sender is on?
)

# Store information about workers.
worker = Table(
    'worker',
    db_metadata,
    Column('user_id', String(63), ForeignKey(user.c.user_id), primary_key=True, nullable=False),
    Column('worker_id', String(127), primary_key=True, nullable=False),
    Column('group_uuid', String(63), ForeignKey(group.c.uuid), nullable=True),
    Column('tag', Text, nullable=True),  # Tag that allows for scheduling runs on specific workers.
    Column('cpus', Integer, nullable=False),  # Number of CPUs on worker.
    Column('gpus', Integer, nullable=False),  # Number of GPUs on worker.
    Column('memory_bytes', BigInteger, nullable=False),  # Total memory of worker.
    Column('free_disk_bytes', BigInteger, nullable=True),  # Available disk space on worker.
    Column(
        'checkin_time', DateTime, nullable=False
    ),  # When the worker last checked in with the bundle service.
    Column('socket_id', Integer, nullable=False),  # Socket ID worker listens for messages on.
    Column(
        'shared_file_system', Boolean, nullable=False
    ),  # Whether the worker and the server have a shared filesystem.
    Column(
        'tag_exclusive', Boolean, nullable=False
    ),  # Whether worker runs bundles if and only if they match tags.
    Column(
        'exit_after_num_runs', Integer, nullable=False
    ),  # Number of jobs allowed to run on worker.
    Column('is_terminating', Boolean, nullable=False),
)

# Store information about all sockets currently allocated to each worker.
worker_socket = Table(
    'worker_socket',
    db_metadata,
    Column('user_id', String(63), ForeignKey(user.c.user_id), nullable=False),
    Column('worker_id', String(127), nullable=False),
    # No foreign key constraint on the worker table so that we can create a socket
    # for the worker before adding the worker to the worker table.
    Column('socket_id', Integer, primary_key=True, nullable=False),
)

# Store information about the bundles currently running on each worker.
worker_run = Table(
    'worker_run',
    db_metadata,
    Column('user_id', String(63), ForeignKey(user.c.user_id), nullable=False),
    Column('worker_id', String(127), nullable=False),
    ForeignKeyConstraint(['user_id', 'worker_id'], ['worker.user_id', 'worker.worker_id']),
    Column('run_uuid', String(63), ForeignKey(bundle.c.uuid), nullable=False),
    Index('uuid_index', 'run_uuid'),
)

# Store information about the dependencies available on each worker.
worker_dependency = Table(
    'worker_dependency',
    db_metadata,
    Column('user_id', String(63), ForeignKey(user.c.user_id), primary_key=True, nullable=False),
    Column('worker_id', String(127), primary_key=True, nullable=False),
    ForeignKeyConstraint(['user_id', 'worker_id'], ['worker.user_id', 'worker.worker_id']),
    # Serialized list of dependencies for the user/worker combination.
    # See WorkerModel for the serialization method.
    Column('dependencies', LargeBinary, nullable=False),
)

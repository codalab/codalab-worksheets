
import sys
import os

from alembic import context
from sqlalchemy import engine_from_config, pool
from logging.config import fileConfig

#add the root dir to the path, so we can import codalab things
CODALAB_CLI_ROOT = os.path.abspath(os.path.split(os.path.split(__file__)[0])[0])
sys.path.append(CODALAB_CLI_ROOT)

from codalab.model import tables
from codalab.lib.codalab_manager import CodaLabManager


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = tables.db_metadata
# allows
# $ alembic revision --autogenerate -m "Added some table"

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    manager = CodaLabManager()
    url = manager.model().engine.url
    context.configure(url=url, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    manager = CodaLabManager()
    engine = manager.model().engine

    connection = engine.connect()
    context.configure(
                connection=connection,
                target_metadata=target_metadata
                )

    try:
        with context.begin_transaction():
            context.run_migrations()
    finally:
        connection.close()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()


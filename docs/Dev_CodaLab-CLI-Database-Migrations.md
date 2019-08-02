## Database migrations

Migrations are handled with [Alembic](http://alembic.readthedocs.org/en/latest/).

If you are planning to add a migration, please check whether:

* You have a fresh DB with no migrations, or
* You have already done a migration and wish to add/upgrade to another.

By running this command:

    venv/bin/alembic current

If you have a migration, it will show you your last migration (head).  (In this
case it's `341ee10697f1`.)

    INFO  [alembic.migration] Context impl SQLiteImpl.
    INFO  [alembic.migration] Will assume non-transactional DDL.
    Current revision for sqlite:////Users/Dave/.codalab/bundle.db: 531ace385q2 -> 341ee10697f1 (head), name of migration

If the DB has no migrations and is all set, the output will be:

    INFO  [alembic.migration] Context impl SQLiteImpl.
    INFO  [alembic.migration] Will assume non-transactional DDL.
    Current revision for sqlite:////Users/Dave/.codalab/bundle.db: None

##### You have a fresh DB with no migrations.

Simply stamp your current to head and add your migration:

    venv/bin/alembic stamp head

##### You have already done a migration and wish to upgrade to another.

    venv/bin/alembic upgrade head

[TODO write about edge cases]

### Adding a new migration

1. Make modifications to the database schema in `tables.py`.

2. If necessary, update COLUMNS in the corresponding ORM objects (e.g., `objects/worksheet.py`).

3. Add a migration:

        venv/bin/alembic revision -m "<your commit message here>" --autogenerate

This will handle most use cases but **check the file it generates**.  If it is
not correct please see the [Alembic
Docs](http://alembic.readthedocs.org/en/latest/tutorial.html#create-a-migration-script)
for more information on the migration script.

4. Upgrade to your migration (modifies the underlying database):

        venv/bin/alembic upgrade head
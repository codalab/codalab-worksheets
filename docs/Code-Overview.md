This document provides an overview of the components of CodaLab and should be read by anyone who wants to contribute to CodaLab.

## Core backend

### Database

#### Schema ([tables.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/model/tables.py))

This is a good place to start to understand the underlying data model in CodaLab.

**Bundles**:

- bundle(uuid, command): contains core information
- bundle_metadata: contains the optional, extensible information in a bundle
- bundle_dependency: stores information about dependencies

**Worksheets**:

- worksheet(uuid, name)
- worksheet_item: A worksheet conceptually is a list of worksheet items, which are either bundles, worksheets, markup or directive
- worksheet_tag: the tags of a worksheet

**Users/groups**:

- user: information about a given user (name, affiliation, etc.)
- group: information about a group, owned by a user
- user_group: assignment of users to groups
- group_bundle_permission: groups have permissions on bundles
- group_object_permission: groups have permissions on worksheets

**Workers**:

- worker: information about a worker that has checked in
- worker_run: a bundle running on a worker
- worker_dependency: which bundles are available on a worker

Technically we use [SQLAlchemy](https://www.sqlalchemy.org/) to allow Python to
interface with the underlying database, but we don’t use it in the standard
way.  Instead of having Python objects representing bundles and worksheets, we
use dictionaries and write raw SQL queries.  See
[bundle_model.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/model/bundle_model.py).

#### Migrations

We use [alembic](https://alembic.sqlalchemy.org/en/latest/tutorial.html) to
change the database schema.  Our migrations are in
[alembic/versions](https://github.com/codalab/codalab-worksheets/tree/master/alembic).

#### Storage

The actual bundle contents are stored on the file system (later, Azure blob storage or Amazon S3)
with a
[bundle_store.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/lib/bundle_store.py)
interface.

### REST API

Both the website and the CLI call the REST server to fetch information.
The automatically generated documentation of the REST API.
The important endpoints are defined in code here:

- [bundles.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/rest/bundles.py):
  getting/updating information about bundles
- [worksheets.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/rest/worksheets.py):
  getting/updating information about worksheets
- [interpret.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/rest/interpret.py):
  performing worksheet interpretation, which should eventually be merged into
  worksheets.py.

The payload returned by the REST API are defined by marshmallow and JSON API,
which allows us to declaratively define the type of data that will be returned.
The [marshmallow schemas](https://marshmallow.readthedocs.io/en/3.0/) are defined in
[schemas.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/rest/schemas.py)
and
[worksheet_block_schemas.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/rest/worksheet_block_schemas.py).

### Worksheets

Recall that a worksheet is a list of worksheet items, where each item is
markup, a bundle reference, a worksheet reference, or a **directive**.  It is this
last category that makes markdown more complex and powerful and is almost like
a mini-programming language.  See what the functionality looks like from the
[user's perspective](https://codalab-worksheets.readthedocs.io/en/latest/Worksheet-Markdown).

**Worksheet interpretation**
([worksheet_util.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/lib/worksheet_util.py))
corresponds to taking (raw) worksheet items and converting it into a list of
**blocks**, which can be visualized by a frontend.

The two most common types of blocks are markup (comprised of a contiguous
sequence of markup worksheet items) and table (conceptually a contiguous list
of bundles), but things get complicated with tables.

First, a table has a **schema**, which specifies what the columns are.  Each
schema is defined using directives (`% schema foo`, `% add`).  Each schema item
specifies which (part of) a file to fetch to show the contents of a cell for a
row (bundle).

Second, a table could be defined by a list of bundles (raw markdown items) or a
search directive, which returns a list of bundles.

## Frontend

### CLI

The main entry point to performing actions on CodaLab
([bundle_cli.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/lib/bundle_cli.py)).

This is a great point to start and trace through how various actions (e.g.,
creating a run) are carried out in the backend.

A CLI can interact with multiple CodaLab instances, and maintain aliases and
current worksheet for each one.  This is captured by the
[codalab_manager.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/lib/codalab_manager.py).

The CLI
([cl.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/bin/cl.py))
is the entry point for running the REST server and bundle manager (and should
include the worker too for uniformity).

### Website

The frontend is what is actually deployed to [https://worksheets.codalab.org](https://worksheets.codalab.org). It uses React and [Material UI](https://material-ui.com/).

The major components are in
[src/components](https://github.com/codalab/codalab-worksheets/tree/master/frontend/src/components).

The main pages are:

- Home page
  ([HomePage.js](https://github.com/codalab/codalab-worksheets/blob/master/frontend/src/routes/HomePage.js)):
  landing page.
- Worksheet view
  ([Worksheet.js](https://github.com/codalab/codalab-worksheets/blob/master/frontend/src/components/worksheets/Worksheet/Worksheet.js)): the page that allows users to render and edit worksheets as well as
  the bundles inside them. Most of the time the user spends is on this page.
- Bundle view
  ([Bundle.js](https://github.com/codalab/codalab-worksheets/blob/master/frontend/src/components/Bundle/Bundle.js)): refers
  to the page that shows information about a bundle

Many of the calls from the website to the REST server are through a mega REST
endpoint
([rest/cli.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/rest/cli.py)).
This seems a bit roundabout, but the rationale is to have a common way of
performing actions on CodaLab, and this gives the opportunity for people using
the frontend to be educated about the CLI (since commands for creating new
worksheets, runs, etc. are converted into CLI commands).

## Worker system

A **worker** contacts the CodaLab server to get bundles to run.  The worker
will download dependencies and Docker images and run bundles in Docker
containers, and periodically report on the status of the runs.

A worker also has to respond to various commands such as reading files in the
bundle while it's running, killing bundles, etc.

### Bundle manager

When a **run bundle** is created, it transitions between states from
**created** to **ready** or **failed**.

The bundle manager
([bundle_manager.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/server/bundle_manager.py))
runs on the server in the background in a loop.  Its duties are:

- Move bundles between various stages (`created` to `staged` when dependencies are ready) and when bundles time out.
- Assign run bundles to workers based on resources and priorities (scheduling logic).
- Create make bundles (trivial thing to do, but someone has to do it).

### Worker

The worker's main entry point is
[codalab/worker/main.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker/main.py).

The top-level
[worker.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker/worker.py)
is generic logic.

The worker has a
[DockerImageManager](https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker/docker_image_manager.py)
for maintaining Docker images
and a [DependencyManager](https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker/dependency_manager.py)
for maintaining bundle dependencies, both of which are download asynchronously
and require a cache.

Finally, the
[RunStateMachine](https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker/worker_run_state.py)
does most of the heavy lifting and transitions jobs between different states.

## Deployment

There is one script
[codalab_service.py](https://github.com/codalab/codalab-worksheets/blob/master/codalab_service.py)
that brings up and down an entire CodaLab service.  See [server
setup](Server-Setup.md) for how
to use it.

Any set of services (e.g., frontend, rest-server) are brought up using Docker
Compose
([compose_files](https://github.com/codalab/codalab-worksheets/tree/master/docker/compose_files)).
These compose files run the actual Docker images
([dockerfiles](https://github.com/codalab/codalab-worksheets/tree/master/docker/dockerfiles)).

### Monitor script

The monitor script
([monitor.py](https://github.com/codalab/codalab-worksheets/blob/master/monitor.py))
runs in a loop to back up the database, gets some statistics about the bundles
and workers, and emails out a report every day.

### Tests

#### Testing overview and philosophy

When you add a new functionality or fix a bug in CodaLab, you **must** add unit tests that test that functionality. We have enforced this constraint by adding Code Coverage checks to CI.

End-to-end tests should be used sparingly and only to test critical functionality and flows. This is because they are much more time-expensive to create and run. Code Coverage checks do not apply to E2E tests.

As we get more code coverage, we should gradually increase the thresholds until we reach 90-100%.

#### Unit tests

- Frontend unit tests in the [frontend/src/__tests__](https://github.com/codalab/codalab-worksheets/tree/master/frontend/__tests__) directory. These tests only test React components and mock out all network calls.

```
cd frontend
npm test
```

Sometimes, if the frontend UI changes and you need to update snapshots, run:

```
npm test -- -u
```

(Note: these are technically "integration tests" since they test multiple components' rendering at once, but let's call them unit tests for simplicity and to distinguish them from the E2E frontend tests).

- Unit tests for the backend in the [tests/unit](https://github.com/codalab/codalab-worksheets/tree/master/tests/unit) directory. These tests mock out certain aspects of the backend to test backend classes / utilities.

To run all tests in the Docker container, run:

```
python3 test_runner.py unittest
```

If you would only like to run specific tests, for example just the worker tests,
on your host machine (for faster iteration), you can run:

```
nosetests tests.unit.worker
```

Note that running the actual REST API tests won't work if you're running it locally using
`nosetests`; they need to be run on the actual Docker container using the `test_runner.py`
command mentioned previously. The other tests should work, though.

#### End-to-end tests

- One end-to-end integration script for the CLI in [tests/cli/test_cli.py](https://github.com/codalab/codalab-worksheets/blob/master/tests/cli/test_cli.py). These tests run an entire CodaLab server and don't mock out anything.

```
python3 test_runner.py default
```

You can update `default` with a specific test name (`unittest`, `run`, etc.) to only run a specific CLI test.

- End-to-end UI tests for the web interface in [tests/ui](https://github.com/codalab/codalab-worksheets/tree/master/tests/ui)

```
python3 test_runner.py frontend
```

- Stress tests in [tests/stress/stress_test.py](https://github.com/codalab/codalab-worksheets/blob/master/tests/stress/stress_test.py)

```
python3 tests/stress/stress_test.py --instance https://worksheets-dev.codalab.org --heavy
```

- Performance tests in [tests/stress/performance_test.py](https://github.com/codalab/codalab-worksheets/blob/master/tests/stress/performance_test.py)

```
python3 tests/stress/performance_test.py --instance https://worksheets-dev.codalab.org
```

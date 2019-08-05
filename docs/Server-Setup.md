It's easy to run your own instance of CodaLab Worksheets!
You may want to do this if you'd like to use CodaLab locally on your laptop or
on your organization's own compute cluster, or if you want to develop and test
CodaLab features.

# Requirements

All you need is a version of Docker and docker-compose compatible with docker-compose file version 3.5.
Note that while Docker/docker-compose should be cross-platform, we have only tested Ubuntu.
If you try to use CodaLab on MacOS or Windows and run into issues, feel free to let us know, but those platforms
are not officially supported at the moment.

* [docker](https://docs.docker.com/install/) version 17.12.0+
* [docker-compose](https://docs.docker.com/compose/install/) version 1.18.0+

Get the source code:

    git clone https://github.com/codalab/codalab-worksheets

# Running

If you're in a hurry, you can start the CodaLab service (frontend, backend, one
worker) with one command:

    ./codalab_service.py start

You can then go to `http://localhost`, sign in with the root `codalab` user
(password `codalab`), try running some bundles.  Normally, you'd install the
CLI using `pip`, but to use the version of the CLI from the repo, you can run
it in Docker:

    docker exec codalab_rest-server_1 cl run date

That's it!

Note that a full deployment of CodaLab service consists of the
following Docker containers running the associated images, which you can see
by running `docker ps`:

Docker Container | Docker Image Used            | Purpose
-----------------|------------------------------|-------------------------------------------------
 frontend        | `codalab/frontend:<version>` | Website (serves static pages)
 rest-server     | `codalab/server:<version>`   | REST API endpoint (used by website and CLI)
 bundle-manager  | `codalab/server:<version>`   | Schedules bundles to workers in the background
 nginx           | `nginx:1.12.0`               | Routes requests to frontend or rest-server
 mysql           | `mysql/mysql:5.53`           | Database for users/bundles/worksheets
 worker          | `codalab/worker:<version>`   | Runs bundle in a Docker container

If you run `docker ps`, you should see a list of Docker containers like this
(by default, we have `--instance-name codalab`):

* `codalab_rest-server_1`
* `codalab_bundle-manager_1`
* `codalab_frontend_1`
* `codalab_mysql_1`
* `codalab_worker_1`
* `codalab_nginx_1`

There are two use cases going forward: (i) development (you're trying to modify
CodaLab) and (ii) productionization (you want to deploy this as a system that
people will use).  Each will build on this basic framework in a different way.

# Development

If you're actively developing and want to test your changes, add the following two flags:

- `-b` (`--build-locally`): builds the Docker images above based on your local
  code.  Otherwise, by default, the public images on
  [DockerHub](https://hub.docker.com/u/codalab) will be used.
- `-d` (`--dev`): runs the development version of the frontend so that your
  changes will be propagated instantly rather than having to rebuild any docker images.
- `-v <name>` (optional): tag the Docker images so that you can play with
  different versions of CodaLab.

Start the CodaLab service as follows:

    ./codalab_service.py start -b -d -v local

If you modify the frontend, you can do so without restarting.  If you would
like to modify the rest server, bundle manager, or worker, then you can edit
the code and then start only that single Docker container.  For example, for
the worker, the command would be:

    ./codalab_service.py start -b -d -v local -s worker

To stop all the Docker containers associated with the CodaLab service (but preserve all the data):

    ./codalab_service.py stop

If you want to delete all the data associated with this, then do:

    ./codalab_service.py delete

## Building Docker images

If you just want to build the Docker images without starting the CodaLab service:

    ./codalab_service.py build -v local

We provide two default Docker images that bundles are run in if no Docker image
is specified, one for GPU jobs and one for non-GPU jobs:

- `codalab/default-cpu:<version>`
- `codalab/default-gpu:<version>`

If you would like to build these as well (note that this might take up to an
hour because lots of packages have to be installed):

    ./codalab_service.py build all -v local

## Testing

To run the tests against an instance that you've already set up:

    ./codalab_service.py start -s test

Or to run a specific test (e.g., basic):

    docker exec codalab_rest-server_1 python test_cli.py basic

You can also start an instance and run tests on it:

    ./codalab_service.py start -b -d -v local -s default test

To fix any style issues for the Python code:

    virtualenv -p python3.6 venv3.6
    venv3.6/bin/pip install black
    venv3.6/bin/black codalab worker *.py --diff

These must pass before you submit a PR.

## Debugging

You can check the logs using standard Docker commands.  For example, if you want to know what the worker is doing:

    docker logs codalab_worker_1 --tail 100 -f

Or to see the logs of all the Docker containers:

    ./codalab_service.py logs --tail 100 -f

You can execute commands in the Docker images to see what's going on, for example:

    docker exec codalab_worker_1 ls /home/codalab/bundles

## Database migrations

If you just want to update your database, run the following command (which includes something to update the database schema via `alembic`):

    ./codalab_service.py start -b -d -v local -s update

If you want to modify the database schema, use `alembic` to create a migration.  Note that everything must be run in Docker, but your modifications are outside in your local codebase.

1. Modify `codalab/model/tables.py` in your local codebase to the desired schema.

1. Rebuild the Docker image for the rest server:

        ./codalab_service.py start -b -d -v local -s rest-server

1. Auto-generate the migration script:

        docker exec codalab_rest-server_1 alembic revision --autogenerate -m "description of your migration"

1. The migration script is created in the Docker image (the file name of the script is printed to stdout, call it <file>), which you need to copy out into your local codebase:

        docker cp codalab_rest-server_1:/opt/codalab-worksheets/alembic/versions/<file> alembic/versions

1. Modify the migration script <file> as necessary.

1. Rebuild the Docker image:

        ./codalab_service.py start -b -d -v local -s rest-server

1. Apply the migration to change the actual database:

        docker exec codalab_rest-server_1 alembic upgrade head

1. Check that the migration was successful by looking at the database schema (use `desc <table>`):

        docker exec -ti codalab_mysql_1 bash
        mysql -u codalab -p codalab_bundles   # Type in `codalab` as the password

# Production

If you want to make the CodaLab instance more permanent and exposed to a larger set of users, there are a couple
details to pay attention to.

## Persistent Storage

By default data files are stored in ephemeral Docker volumes. It's a good idea
to store Codalab data on a persistent location on your host machine's disk if
you're running real workflows on it. Here are a few configuration options you
might want to set for a real-use persistent instance:

- `--codalab-home`: Path to store server configuration and bundle data files.
- `--mysql-mount`: Path to store DB configuration and data files.
- `--external-db-url`: If you want to run your DB on another machine, this is the URL to connect to that database. You can set the user and password for this database using the `--mysql-user` and `--mysql-password` arguments.
- `--worker-dir`: Path to store worker configuration and temporary data files.
- `--bundle-store`: [EXPERIMENTAL] Another path to store bundle data. You can add as many of these as possible and bundle data will be distributed evenly across these paths. Good for when you mount multiple disks to distribute bundle data. WARNING: This is not fully supported and tested yet, but support is under development.

## Security and Credentials

By default, a lot of credentials are set to unsafe defaults (`"codalab"`).  You
should override these with more secure options.  Here's a list of all
credential options:

* `--codalab-username` [codalab]: Username of the admin account on the CodaLab platform
* `--codalab-password` [codalab]: Password of the admin account on the CodaLab platform
* `--mysql-root-password` [codalab]: Root password for the MYSQL database.
* `--mysql-user` [codalab]: MYSQL username for the CodaLab account on the MYSQL database
* `--mysql-password` [codalab]: MYSQL password for the CodaLab account on the MYSQL database

### SSL

By default the web interface is served over HTTP. If you have certificates for
your domain, you can serve over HTTPS as well. To do so:

1. Ensure you have the certificate and key files in a path accessible to the service.
2. Set the following options when starting the CodaLab service, so that the
   website will be served over Port 443 using SSL:

    * `--use-ssl`: If specified, use SSL
    * `--ssl-cert-file`: Path to the certificate file
    * `--ssl-key-file`: Path to the key file

## Ports to be exposed

Normally the service exposes a minimal number of port outside the Docker
network.  if for whatever reason you want direct access to the individual ports
of the services, you can expose these at host ports of your choosing.

* `--rest-port` [2900]: Port for REST API
* `--http-port` [80]: Port to serve HTTP from (nginx)
* `--mysql-port` [3306]: Port to expose the MySQL database
* `--frontend-port`: Port to serve the React frontend

# Advanced customization

## Multiple instances

If for some reason you need to start more than one instance of the CodaLab
service on the same machine, be careful about the following:

* Use the `--instance-name` option with all commands: The default name is
  `codalab` but you should give other instances distinct names and use that
  argument any time you want to interact with that instance.
* Avoid disk mount clashing: If you're using host machine disk mounts to store
  data, make sure mountpoints are different for different instances.
* Avoid port clashing: If you're exposing ports, make sure you set different
  ports for different instances, at the very least you need to configure the
  `http-port` of later instances to something other than `80`.

## Custom docker compose file

For less common use cases, you might want to get your feet wet in Docker and
docker-compose and provide a custom docker-compose file to override our
configurations. You can use the `--user-compose-file` option to include a
custom docker-compose file that will override any configuration you want. To
understand our `compose` setup, please look into the source of
`codalab_service.py` and the docker-related files in the `./docker/` directory.

## Configuration with environment variables

Some of the CodaLab service options can be set via environment variables
instead of command-line options. This is useful for sensitive options like
passwords, and also useful for setting local defaults instead of reusing long
argument lists.

Here's a list of options that can be configured via environment variables and
the corresponding environment variable names:

* `version             : CODALAB_VERSION`
* `dev                 : CODALAB_DEV`
* `user_compose_file   : CODALAB_USER_COMPOSE_FILE`
* `start_worker        : CODALAB_START_WORKER`
* `mysql_root_password : CODALAB_MYSQL_ROOT_PWD`
* `mysql_user          : CODALAB_MYSQL_USER`
* `mysql_password      : CODALAB_MYSQL_PWD`
* `codalab_user        : CODALAB_ROOT_USER`
* `codalab_password    : CODALAB_ROOT_PWD`
* `codalab_home        : CODALAB_SERVICE_HOME`
* `mysql_mount         : CODALAB_MYSQL_MOUNT`
* `worker_dir          : CODALAB_WORKER_DIR`
* `http_port           : CODALAB_HTTP_PORT`
* `rest_port           : CODALAB_REST_PORT`
* `frontend_port       : CODALAB_FRONTEND_PORT`
* `mysql_port          : CODALAB_MYSQL_PORT`
* `ssl_cert_file       : CODALAB_SSL_CERT_FILE`
* `ssl_key_file        : CODALAB_SSL_KEY_FILE`

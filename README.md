# CodaLab Worksheets
[![Build Status](https://travis-ci.org/codalab/codalab-worksheets.svg?branch=master)](https://travis-ci.org/codalab/codalab-worksheets.svg?branch=master)
[![Downloads](https://pepy.tech/badge/codalab)](https://pepy.tech/project/codalab)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PyPI version](https://badge.fury.io/py/codalab.svg)](https://badge.fury.io/py/codalab)

The goal of CodaLab Worksheets is to faciliate transparent, reproducible, and
collaborative research in computation- and data-intensive areas such as machine
learning.

The CodaLab Bundle Service allows users to create *bundles*, which are
immutable directories containing code or data.  Bundles are either
uploaded or created from other bundles by executing arbitrary commands.
When the latter happens, all the provenance information is preserved.  In
addition, users can create *worksheets*, which interleave bundles with
free-form textual descriptions, allowing one to easily describe an experimental
workflow.

The CodaLab frontend holds the React front-end web interface for CodaLab worksheets.

To get started visit [the official CodaLab Worksheets instance](https://worksheets.codalab.org/)
For more information about the platform, visit [our Wiki](https://github.com/codalab/codalab-worksheets/wiki)

If you're interested in contributing or setting up your own CodaLab Worksheets instance, [get in touch with us](mailto:codalab.worksheets@gmail.com)


## Links

* [Official CodaLab Worksheets Instance](https://worksheets.codalab.org/): live instance of CodaLab Worksheets
* [CodaLab Worksheets Wiki](https://github.com/codalab/codalab-worksheets/wiki): all documentation

## Bringing up your own instance of CodaLab Worksheets

We provide a convenience script that uses `docker-compose` to bring up a full fledged CodaLab server in this repo.
To get started, make sure you have a recent version of Docker and docker-compose installed.
Simply run `bash docker/codalab_service/start_service.sh -i` to bring up a fresh instance of CodaLab with the default configuration.
If you've made local changes to the codebase and would like to rebuild docker images from the current state of the codebase, 
use `bash docker/codalab_service/start_service.sh -i -b`.
If you've previously started an instance and thus have the database and root account initialization done, omit the `-i` flag to just bring the service up like so: `bash docker/codalab_service/start_service.sh`

You can customize variables such as the root account credentials, where the bundle store and home folder and mysql data are mounted in your local filesystem. To see a full list of these variables and their names, consult `docker/codalab_service/start_service.sh`.


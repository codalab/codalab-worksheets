# CodaLab Bundle Service
[![Build Status](https://travis-ci.org/codalab/codalab-cli.svg?branch=master)](https://travis-ci.org/codalab/codalab-cli.svg?branch=master)
[![Downloads](https://pepy.tech/badge/codalab)](https://pepy.tech/project/codalab)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PyPI version](https://badge.fury.io/py/codalab.svg)](https://badge.fury.io/py/codalab)

The goal of CodaLab is to faciliate transparent, reproducible, and
collaborative research in computation- and data-intensive areas such as machine
learning.  This repository contains the code for the CodaLab Bundle Service,
which provides the backend for [CodaLab Worksheets](https://github.com/codalab/codalab-worksheets/wiki).

The CodaLab Bundle Service allows users to create *bundles*, which are
immutable directories containing code or data.  Bundles are either
uploaded or created from other bundles by executing arbitrary commands.
When the latter happens, all the provenance information is preserved.  In
addition, users can create *worksheets*, which interleave bundles with
free-form textual descriptions, allowing one to easily describe an experimental
workflow.

This package also contains a command-line interface (CLI) `cl` that provides
flexible access to the CodaLab Bundle Service.  The [Git repository for the
CodaLab website](https://github.com/codalab/codalab-worksheets) is provides a graphical
interface to the service.

## Links

* [Official CodaLab Worksheets Instance](https://worksheets.codalab.org/): live instance of CodaLab Worksheets
* [CodaLab Worksheets Wiki](https://github.com/codalab/codalab-worksheets/wiki): all documentation

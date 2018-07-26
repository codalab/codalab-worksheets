# CodaLab Bundle Service
[![Build Status](https://travis-ci.org/codalab/codalab-cli.png?branch=master)](https://travis-ci.org/codalab/codalab-cli.png?branch=master)


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

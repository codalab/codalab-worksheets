# CodaLab Bundle Service [![Build Status](https://travis-ci.org/codalab/codalab-cli.png?branch=master)](https://travis-ci.org/codalab/codalab-cli)

The goal of CodaLab is to faciliate transparent, reproducible, and
collaborative research in computation- and data-intensive areas such as machine
learning.  Think Git for experiments.  This repository contains the code for
the CodaLab Bundle Service and provides the foundation on which the [CodaLab
website](https://github.com/codalab/codalab) is built.

The CodaLab Bundle Service allows users to create *bundles*, which are
immutable directories containing code or data.  Bundles are either
uploaded or created from other bundles by executing arbitrary commands.
When the latter happens, all the provenance information is preserved.  In
addition, users can create *worksheets*, which interleave bundles with
free-form textual descriptions, allowing one to easily describe an experimental
workflow.

This package also contains a command-line interface `cl` that provides flexible
access to the CodaLab Bundle Service.  The [CodaLab
website](https://github.com/codalab/codalab) provides a graphical interface to
the service, as well as supporting competitions.

Try the CLI now online on [CodaLab.org](http://codalab.org/).

## Documentation
* [CodaLab Wiki](https://github.com/codalab/codalab/wiki)
* [CodaLab CLI Tutorial](https://github.com/codalab/codalab/wiki/User_CodaLab%20CLI%20Tutorial)

## Community

The CodaLab community forum is hosted on Google Groups.
* [CodaLabDev Google Groups Forum](https://groups.google.com/forum/#!forum/codalabdev)

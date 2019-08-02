Every run bundle is created by executing a command in a
[Docker](https://www.docker.com/) container, which provides a standardized
Linux environment that is lighter-weight than a full virtual machine.

You need to install Docker if you intend to execute CodaLab runs yourself.
There are two use cases:

1. You have set up [your own CodaLab server](Server-Setup).
2. You want to run [your own worker](Execution#running-your-own-worker) that
   connects to the main CodaLab server ([worksheets.codalab.org](https://worksheets.codalab.org)).

To install Docker, visit the official [Docker installation instructions](https://docs.docker.com/engine/installation/#docker-variants) or, if you're using a Linux distro, run `curl -sSL https://get.docker.com/ | sh`.
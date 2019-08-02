## Unit tests

To run tests on the code, first install the libraries for testing:

    venv/bin/pip install mock nose

Then run all the tests:

    venv/bin/nosetests

## End-to-end tests

Make sure you connect to the desired CodaLab instance (either `local::` or `localhost::`).  Then run the following command to tests the CodaLab CLI and bundle service:

    ./test-cli.py all
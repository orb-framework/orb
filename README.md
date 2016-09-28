orb
======================

The ORB library is an object-oriented, pythonic, object relational mapper
for databases.

Installation
-----------------------

If you would like to use the latest build that has been tested and published,
you can use the Python `setuptools` to install it to your computer or virtual
environment:

    pip install orb-api

If you would like to use the latest code base, you can clone the repository
and reference and run `setup.py` for your virtual environment

    git clone https://github.com/orb-framework/orb.git
    cd orb
    python setup.py develop

Running Tests
-----------------------

To run the unit tests, you can just run the test command from the setuptools

    python setup.py test

To run the functional tests, you will need to specify the test environment using the
[tox](https://testrun.org/tox/latest/) command line utility.

    pip install tox
    tox -e postgres
   

Documentation
---------
Documentation for orb is written and hosted in [GitBook](https://www.gitbook.com/book/orb-framework/orb)

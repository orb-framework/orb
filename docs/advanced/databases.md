Databases
=================
Currently we support a number of backend classes.  This is a list of the available backends and their requirements.

SQL Based Backends
=================

SQLite
-----------------
The backend requirements are built-in to the Python binary.

MySQL
-----------------
Requires the [PyMySQL](https://github.com/PyMySQL/PyMySQL) package.

PostgreSQL
-----------------
Requires the [psycopg2](http://initd.org/psycopg/) package.

    from orb import Database
    db = Database('Postgres', 'my_database')
    db.setUsername('postgres')
    db.setPassword('x')
    db.setHost('localhost')
    db.set

Non-SQL Based Backends
=================

MongoDB
-----------------
Requires the [mongodb](http://www.mongodb.org/) package.

JSONRPC
-----------------

Quickbase
-----------------
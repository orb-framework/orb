import os
import pytest

test_backends = set()

# setup sqlite testing
if os.environ.get('IGNORE_SQLITE') != '1':
    test_backends.add('sqlite')

# setup postgres testing
if os.environ.get('IGNORE_POSTGRES') != '1':
    try:
        import psycopg2
    except ImportError:
        pass
    else:
        test_backends.add('postgres')

# setup MySQL testing
if os.environ.get('IGNORE_MYSQL') != '1':
    try:
        import pymysql
    except ImportError:
        pass
    else:
        test_backends.add('mysql')

# setup the requirement markers
requires_mysql = pytest.mark.skipif('mysql' not in test_backends, reason='Ignoring MySQL')
requires_pg = pytest.mark.skipif('postgres' not in test_backends, reason='Ignoring PostgreSQL')
requires_lite = pytest.mark.skipif('sqlite' not in test_backends, reason='Ignoring SQLite')

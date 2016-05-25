import pytest

try:
    import psycopg2 as pg
except ImportError:
    pg = None

try:
    import sqlite3 as sqlite
except ImportError:
    sqlite = None

try:
    import pymysql as mysql
except ImportError:
    mysql = None

requires_pg = pytest.mark.skipif(pg is None, reason='psycopg2 required for Postgres')
requires_mysql = pytest.mark.skipif(mysql is None, reason='PyMySQL required for MySQL')
requires_lite = pytest.mark.skipif(sqlite is None, reason='sqlite3 required for SQLite')

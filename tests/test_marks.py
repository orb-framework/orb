import pytest

try:
    import psycopg2 as pg
except ImportError:
    pg = None

try:
    import sqlite3 as sqlite
except ImportError:
    sqlite = None


requires_pg = pytest.mark.skipif(pg is None, reason='psycopg2 required for Postgres')
requires_lite = pytest.mark.skipif(sqlite is None, reason='sqlite3 required for SQLite')

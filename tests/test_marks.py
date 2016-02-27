import pytest

try:
    import psycopg2 as pg
except ImportError:
    pg = None


requires_pg = pytest.mark.skipif(pg is None, reason='psycopg2 required for Postgres')

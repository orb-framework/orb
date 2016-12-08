import pytest


def test_sql_connection_is_abstract():
    from orb.std.connections.sql.connection import SQLConnection

    with pytest.raises(Exception):
        assert SQLConnection() is None


def test_sql_connection_alter_model(mock_sql_conn):
    conn = mock_sql_conn()
    assert conn is not None
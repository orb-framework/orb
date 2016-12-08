def test_sql_connection_is_abstract():
    from orb.std.connections.sql.connection import SQLConnection

    assert SQLConnection()
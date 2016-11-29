def test_my_loaded(orb):
    from orb.std.connections.sql.postgres import PSQLConnection
    assert orb.Connection.get_plugin('Postgres') == PSQLConnection

def test_my_db_sync(orb, my_db, testing_schema, Comment, TestAllColumns):
    my_db.sync()
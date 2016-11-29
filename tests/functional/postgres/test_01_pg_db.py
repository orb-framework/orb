def test_pg_loaded(orb):
    from orb.std.connections.sql.postgres import PSQLConnection
    assert orb.Connection.get_plugin('Postgres') == PSQLConnection

def test_pg_db_sync(orb, pg_db, testing_schema, Comment, TestAllColumns):
    pg_db.sync()
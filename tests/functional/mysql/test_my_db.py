def test_my_loaded(orb):
    from orb.core.connection_types.sql.postgres import PSQLConnection
    assert orb.Connection.byName('Postgres') == PSQLConnection

def test_my_db_sync(orb, my_db, testing_schema, Comment, TestAllColumns):
    my_db.sync()
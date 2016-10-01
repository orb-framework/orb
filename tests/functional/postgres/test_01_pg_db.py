def test_pg_loaded(orb):
    from orb.core.connection_types.sql.postgres import PSQLConnection
    assert orb.Connection.byName('Postgres') == PSQLConnection

def test_pg_db_sync(orb, pg_db, testing_schema, Comment, TestAllColumns):
    pg_db.sync()
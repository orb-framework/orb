from test_marks import requires_pg


@requires_pg
def test_pg_loaded():
    import orb
    from orb.core.connection_types.sql.postgresql import PSQLConnection
    assert orb.Connection.byName('Postgres') == PSQLConnection

@requires_pg
def test_pg_db_connection(pg_db):
    assert pg_db.connect() is not None
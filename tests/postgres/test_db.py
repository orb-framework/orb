def test_pg_addon():
    import orb.core.connection_types.sql.postgresql

def test_pg_db_connection(pg_db):
    assert pg_db.connect() == True
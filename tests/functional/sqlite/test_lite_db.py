def test_clear_lite_db(lite_db):
    import os
    if os.path.exists('orb_testing'):
        os.remove('orb_testing')

def test_lite_loaded(orb):
    from orb.core.connection_types.sql.sqlite import SQLiteConnection
    assert orb.Connection.byName('SQLite') == SQLiteConnection

def test_lite_db_sync(orb, lite_db, testing_schema, TestAllColumns):
    lite_db.sync()
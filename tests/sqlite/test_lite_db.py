import pytest
from test_marks import requires_lite


@pytest.mark.run(order=1)
@requires_lite
def test_lite_loaded(orb):
    from orb.core.connection_types.sql.sqlite import SQLiteConnection
    assert orb.Connection.byName('SQLite') == SQLiteConnection

@pytest.mark.run(order=1)
@requires_lite
def test_lite_db_connection(lite_db):
    assert lite_db.connect() is not None

@pytest.mark.run(order=1)
@requires_lite
def test_lite_db_sync(orb, lite_db, testing_schema, TestAllColumns):
    lite_db.sync()
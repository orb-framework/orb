import pytest
from tests.test_marks import requires_mysql


@pytest.mark.run(order=1)
@requires_mysql
def test_my_loaded(orb):
    from orb.core.connection_types.sql.postgres import PSQLConnection
    assert orb.Connection.byName('Postgres') == PSQLConnection

@pytest.mark.run(order=1)
@requires_mysql
def test_my_db_sync(orb, my_db, testing_schema, Comment, TestAllColumns):
    my_db.sync()
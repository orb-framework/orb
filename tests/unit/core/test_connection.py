import pytest


def test_create_abstract_connection_fails():
    from orb.core.connection import Connection

    with pytest.raises(TypeError):
        assert not Connection()


def test_connection_plugin_registration():
    from orb.core.connection import Connection

    class TestConnection(Connection):
        __plugin_name__ = 'Test'

    assert Connection.get_plugin('Test') == TestConnection


def test_create_mock_connection():
    from orb.core.database import Database
    from orb.testing import MockConnection

    conn = MockConnection()
    db = Database(conn)

    assert conn.database() == db


def test_deleting_connection_closes_automatically():
    from orb.testing import MockConnection

    check = {}
    def closed(*args):
        check['closed'] = True

    conn = MockConnection(responses={
        'close': closed
    })
    del conn

    assert check['closed']
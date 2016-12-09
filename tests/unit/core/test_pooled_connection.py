import pytest


@pytest.fixture()
def mock_pooled_conn(mock_db):
    from orb.testing import MockPooledConnection

    return MockPooledConnection(mock_db())


def test_pooled_connection_is_abstract():
    import orb

    with pytest.raises(Exception):
        assert orb.PooledConnection() is None


def test_pooled_connection_close(mock_pooled_conn):
    import orb

    with mock_pooled_conn.pool().current_connection() as conn:
        pass

    assert orb.PooledConnection.is_connected(mock_pooled_conn) is True
    orb.PooledConnection.close(mock_pooled_conn)
    assert orb.PooledConnection.is_connected(mock_pooled_conn) is False


def test_pooled_connection_open(mock_pooled_conn):
    import orb
    from orb.testing import MockNativeConnection
    conn = orb.PooledConnection.open(mock_pooled_conn)
    assert isinstance(conn, MockNativeConnection)


def test_pooled_connection_commit(mock_pooled_conn):
    import orb

    assert orb.PooledConnection.commit(mock_pooled_conn) is True


def test_pooled_connection_rollback(mock_pooled_conn):
    import orb

    assert orb.PooledConnection.rollback(mock_pooled_conn) is True

def test_pooled_connection_handles_failed_rollback(mock_pooled_conn):
    import orb
    from orb.testing import MockNativeConnection

    class TestNative(MockNativeConnection):
        def rollback(self):
            raise Exception('uh-oh')

    mock_pooled_conn.__native_class__ = TestNative
    assert orb.PooledConnection.rollback(mock_pooled_conn) is False
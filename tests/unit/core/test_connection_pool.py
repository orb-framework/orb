import pytest


@pytest.fixture()
def mock_pool(mock_db):
    from orb.testing import MockPooledConnection

    conn = MockPooledConnection(mock_db())
    return conn.pool()


def test_connection_pool_construction(mock_pool):
    assert mock_pool.has_connections() is False


def test_connection_pool_context_management(mock_pool):
    from orb.testing import MockNativeConnection

    with mock_pool.current_connection() as conn:
        conn_a = conn

        assert isinstance(conn, MockNativeConnection)
        assert conn.closed is False

        # no active connections in the pool because
        # it is currently being used
        assert mock_pool.has_connections() is False

    # the connection we just opened and used is now
    # back in the pool, and thus available
    assert mock_pool.has_connections() is True

    with mock_pool.current_connection() as conn:
        conn_b = conn

        assert isinstance(conn, MockNativeConnection)
        assert conn.closed is False

        # we're reusing the connection from before,
        # so there are no more in the pool
        assert mock_pool.has_connections() is False

    # the connection has once again been returned to the queue
    assert mock_pool.has_connections() is True

    # ensure that the connection was re-used
    assert conn_a is conn_b


def test_connection_pool_closure_does_not_reenter_pool(mock_pool):
    assert mock_pool.has_connections() is False

    with mock_pool.current_connection() as conn:
        conn_a = conn
        conn.closed = True

    assert mock_pool.has_connections() is False

    with mock_pool.current_connection() as conn:
        conn_b = conn
        conn.closed = True

    assert mock_pool.has_connections() is False
    assert conn_a != conn_b


def test_connection_pool_closure(mock_pool):
    with mock_pool.current_connection():
        pass

    assert mock_pool.has_connections() is True
    mock_pool.close_connections()
    assert mock_pool.has_connections() is False


def test_connection_pool_rolls_back_on_error(mock_pool):
    conn_a = None
    try:
        with mock_pool.current_connection() as conn:
            conn_a = conn
            raise Exception('uh-oh')
    except Exception:
        pass

    assert conn_a.rolled_back


def test_connection_pool_closes_on_error(mock_pool):
    try:
        with mock_pool.current_connection() as conn:
            conn.closed = True
            raise Exception('uh-oh')
    except Exception:
        pass

    assert not mock_pool.has_connections()


def test_connection_pool_with_isolation_level(mock_pool):
    with mock_pool.current_connection(isolation_level=10) as conn:
        conn_a = conn
        assert conn.isolation_level == 10

    assert conn_a.isolation_level == 10

    with mock_pool.current_connection(isolation_level=10) as conn:
        assert conn.isolation_level == 10


def test_connection_pool_with_erroring_native(mock_pool):
    from orb.testing import MockNativeConnection

    class ErroringNative(MockNativeConnection):
        def __init__(self, *args, **kw):
            super(ErroringNative, self).__init__(*args, **kw)
            raise Exception('uh-oh')

    mock_pool._ConnectionPool__connection.__native_class__ = ErroringNative

    try:
        with mock_pool.current_connection() as conn:
            pass
    except Exception:
        pass

    assert mock_pool.has_connections() is False

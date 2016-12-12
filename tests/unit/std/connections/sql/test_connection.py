import pytest


@pytest.fixture()
def mock_sql_conn(mock_db):
    from jinja2 import DictLoader
    from orb.testing import MockPooledConnectionMixin
    from orb.std.connections.sql.connection import SQLConnection

    def _mock_sql_conn(templates=None, execute_response=None):
        import orb

        class MockSQLConnection(MockPooledConnectionMixin, SQLConnection):
            __templates__ = DictLoader(templates or {})

            def execute(self, *args, **kw):
                if execute_response is not None:
                    return execute_response
                else:
                    return super(MockSQLConnection, self).execute(*args, **kw)

        # register types
        MockSQLConnection.register_type_mapping(orb.IdColumn, 'id')
        MockSQLConnection.register_type_mapping(orb.StringColumn, 'string')
        MockSQLConnection.register_type_mapping(orb.ReferenceColumn, 'reference')

        return MockSQLConnection(mock_db())

    return _mock_sql_conn

@pytest.fixture()
def mock_table():
    import orb

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn()

    return User


@pytest.fixture()
def mock_view():
    import orb

    class UserView(orb.View):
        __register__ = False
        __id__ = 'user'

        user = orb.ReferenceColumn('User')

    return UserView


# -----


def test_sql_connection_is_abstract():
    from orb.std.connections.sql.connection import SQLConnection

    with pytest.raises(Exception):
        assert SQLConnection() is None


def test_sql_connection_alter_model(mock_sql_conn, mock_table, mock_view):
    import orb

    empty = mock_sql_conn()
    valid = mock_sql_conn(templates={'alter_table.sql.jinja': ''})
    context = orb.Context()

    assert valid.alter_model(mock_table, context) is True

    with pytest.raises(NotImplementedError):
        assert valid.alter_model(mock_view, context) is False

    with pytest.raises(Exception):
        assert empty.alter_model(mock_table, context) is False


def test_sql_connection_count(mock_sql_conn, mock_table, mock_view):
    import orb

    empty = mock_sql_conn()
    valid = mock_sql_conn(templates={'select_count.sql.jinja': ''},
                          execute_response=([{'count': 5}, {'count': 1}], 2))
    context = orb.Context()

    assert valid.count(mock_table, context) == 6
    assert valid.count(mock_view, context) == 6

    with pytest.raises(Exception):
        assert empty.count(mock_table, context)


def test_sql_connection_create_model(mock_sql_conn, mock_table, mock_view):
    import orb

    empty = mock_sql_conn()
    valid = mock_sql_conn(templates={
        'create_view.sql.jinja': '',
        'create_table.sql.jinja': ''
    })
    context = orb.Context()

    assert valid.create_model(mock_table, context) is True
    assert valid.create_model(mock_view, context) is True

    with pytest.raises(Exception):
        empty.create_model(mock_table, context) is False

    with pytest.raises(Exception):
        empty.create_model(mock_view, context) is False


def test_sql_connection_create_namespace(mock_sql_conn):
    import orb

    empty = mock_sql_conn()
    valid = mock_sql_conn(templates={
        'create_namespace.sql.jinja': ''
    })
    context = orb.Context()

    assert valid.create_namespace('testing', context) is True

    with pytest.raises(Exception):
        assert empty.create_namespace('testing', context) is False


def test_sql_connection_current_schema(mock_sql_conn):
    pass


def test_sql_connection_delete_records(mock_sql_conn, mock_table, mock_view):
    import orb

    empty = mock_sql_conn()
    valid = mock_sql_conn(templates={
        'delete.sql.jinja': '',
        'query.sql.jinja': ''
    })
    context = orb.Context()

    assert valid.delete([mock_table({'id': 1})], context)
    assert valid.delete(orb.Collection(model=mock_table), context)

    with pytest.raises(Exception):
        assert empty.delete([mock_table({'id': 1})], context)

    # cannot delete view instances
    with pytest.raises(NotImplementedError):
        assert valid.delete([mock_view({'user': 1})], context)

    with pytest.raises(NotImplementedError):
        assert valid.delete(orb.Collection(model=mock_view), context)
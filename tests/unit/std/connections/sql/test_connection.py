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
        for column in orb.Column.__subclasses__():
            MockSQLConnection.register_type_mapping(column, column.__name__)

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
    empty = mock_sql_conn()

    assert empty.current_schema(None) == {}


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


def test_sql_connection_insert(mock_sql_conn):
    import orb

    empty = mock_sql_conn()
    valid = mock_sql_conn(templates={
        'insert.sql.jinja': ''
    })
    context = orb.Context()

    records, count = valid.insert([], context)

    assert records == {}
    assert count == 0

    with pytest.raises(Exception):
        assert empty.insert([], context) is False


def test_sql_connection_process_column(mock_sql_conn):
    import orb
    import pprint

    conn = mock_sql_conn()
    schema = orb.Schema(dbname='testing')
    col_a = orb.StringColumn(name='testing', schema=schema)
    col_b = orb.ReferenceColumn(name='created_by', schema=schema)
    col_c = orb.ReferenceColumn(name='created_by', field='fkey_users__user_created_by', schema=schema)

    data_a = conn.process_column(col_a, None)
    data_b = conn.process_column(col_b, None)
    data_c = conn.process_column(col_c, None)

    pprint.pprint(data_a)

    assert data_a == {
        'alias': 'testing',
        'field': 'testing',
        'flags': {},
        'is_string': True,
        'sequence': 'testing_testing_seq',
        'type': 'StringColumn'
    }

    pprint.pprint(data_b)
    assert data_b == {
        'alias': 'created_by_id',
        'field': 'created_by_id',
        'flags': {},
        'is_string': False,
        'sequence': 'testing_created_by_id_seq',
        'type': 'ReferenceColumn'
    }

    pprint.pprint(data_c)
    assert data_c == {
        'alias': 'created_by_id',
        'field': 'fkey_users__user_created_by',
        'flags': {},
        'is_string': False,
        'sequence': 'testing_fkey_users__user_created_by_seq',
        'type': 'ReferenceColumn'
    }


def test_sql_connection_process_index(mock_sql_conn):
    import orb
    import pprint

    conn = mock_sql_conn()

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn()
        first_name = orb.StringColumn()
        last_name = orb.StringColumn()

        by_username = orb.Index(['username'], flags={'Unique'})
        by_display_name = orb.Index(['first_name', 'last_name'])

    idx_a = User.schema().index('by_username')
    idx_b = User.schema().index('by_display_name')

    data_a = conn.process_index(idx_a, None)
    data_b = conn.process_index(idx_b, None)

    pprint.pprint(data_a)
    pprint.pprint(data_b)

    assert data_a == {
        'columns': [{
            'alias': 'username',
            'field': 'username',
            'flags': {},
            'is_string': True,
            'sequence': 'users_username_seq',
            'type': 'StringColumn'
        }],
        'flags': {'Unique': True},
        'name': 'users_by_username_idx'
    }

    assert data_b == {
        'columns': [{
            'alias': 'first_name',
            'field': 'first_name',
            'flags': {},
            'is_string': True,
            'sequence': 'users_first_name_seq',
            'type': 'StringColumn'
        }, {
            'alias': 'last_name',
            'field': 'last_name',
            'flags': {},
            'is_string': True,
            'sequence': 'users_last_name_seq',
            'type': 'StringColumn'
        }],
        'flags': {},
        'name': 'users_by_display_name_idx'
    }


def test_sql_connection_process_model(mock_sql_conn):
    import orb
    import pprint

    conn = mock_sql_conn()
    system = orb.System()

    class User(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        username = orb.StringColumn()
        first_name = orb.StringColumn()
        last_name = orb.StringColumn()

        by_username = orb.Index(['username'], flags={'Unique'})
        by_display_name = orb.Index(['first_name', 'last_name'])

    class Employee(User):
        __system__ = system
        __namespace__ = 'hr'

        manager = orb.ReferenceColumn('Employee')

    data_user = conn.process_model(User, None)
    data_user_alias = conn.process_model(Employee,
                                         orb.Context(namespace='auth'),
                                         aliases={Employee: 'test_alias'})

    pprint.pprint(data_user)
    pprint.pprint(data_user_alias)

    assert data_user == {
        'alias': 'users',
        'force_alias': False,
        'inherits': {},
        'name': 'users',
        'namespace': ''
    }

    assert data_user_alias == {
        'alias': 'test_alias',
        'force_alias': True,
        'inherits': {
            'alias': 'users',
            'force_alias': False,
            'inherits': {},
            'name': 'users',
            'namespace': 'auth'
        },
        'name': 'employees',
        'namespace': 'hr'
    }


def test_sql_connection_process_query(mock_sql_conn):
    import orb
    import pprint

    conn = mock_sql_conn()

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn()

    q = orb.Query('username') == 'jdoe'
    kw, data = conn.process_query(User, q, orb.Context(namespace='public'))

    value_key = data.keys()[0]

    pprint.pprint(kw)
    assert kw == {
        'case_sensitive': False,
        'column': {
            'alias': 'username',
            'field': 'username',
            'flags': {},
            'is_string': True,
            'sequence': 'users_username_seq',
            'type': 'StringColumn'
        },
        'field': u'"public"."users"."username"',
        'inverted': False,
        'op': 'IS',
        'op_name': 'Is',
        'value': {
            'id': value_key,
           'key': u'%({0})s'.format(value_key),
           'variable': 'jdoe'}
    }



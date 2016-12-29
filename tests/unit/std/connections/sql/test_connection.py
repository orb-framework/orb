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


def test_sql_connection_process_value(mock_sql_conn):
    import orb

    conn = mock_sql_conn()

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn()

    column = User.schema().column('username')

    value_a, _ = conn.process_value(column, orb.Query.Op.Is, 'jdoe')
    value_b, _ = conn.process_value(column, orb.Query.Op.Startswith, 'jd')
    value_c, _ = conn.process_value(column, orb.Query.Op.DoesNotStartwith, 'jd')
    value_d, _ = conn.process_value(column, orb.Query.Op.Endswith, 'oe')
    value_e, _ = conn.process_value(column, orb.Query.Op.DoesNotEndwith, 'oe')
    value_f, _ = conn.process_value(column, orb.Query.Op.Contains, 'do')
    value_g, _ = conn.process_value(column, orb.Query.Op.Contains, 'do')

    assert value_a == 'jdoe'
    assert value_b == 'jd%'
    assert value_c == 'jd%'
    assert value_d == '%oe'
    assert value_e == '%oe'
    assert value_f == '%do%'
    assert value_g == '%do%'


def test_sql_connection_process_value_as_query(mock_sql_conn):
    import orb

    conn = mock_sql_conn()

    class User(orb.Table):
        __namespace__ = 'auth'
        __system__ = orb.System()

        id = orb.IdColumn()
        username = orb.StringColumn()
        manager = orb.ReferenceColumn('User')

    column = User.schema().column('manager')

    value_a, _ = conn.process_value(column, orb.Query.Op.Is, orb.Query(User, 'id') == 1)

    assert value_a == '"auth"."users"."id"'


def test_sql_connection_process_value_as_collection(mock_sql_conn):
    import orb
    import pprint

    class User(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        username = orb.StringColumn()
        manager = orb.ReferenceColumn('User')

    column = User.schema().column('manager')
    u = User({'id': 1})
    u.mark_loaded()

    empty = mock_sql_conn()
    valid = mock_sql_conn(templates={
        'select.sql.jinja': ''
    })

    value_a, _ = valid.process_value(column, orb.Query.Op.IsIn, orb.Collection())
    value_b, data_b = valid.process_value(column, orb.Query.Op.IsIn, orb.Collection([u]))

    pprint.pprint(value_b)
    pprint.pprint(data_b)

    assert value_a == []
    assert value_b == ''
    assert data_b == {}

    with pytest.raises(Exception):
        empty.process_value(column, orb.Query.Op.IsIn, orb.Collection([u]))


def test_sql_connection_render_alter_table(mock_sql_conn, sql_equals):
    import orb

    class Test(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        standard = orb.StringColumn()
        translated = orb.StringColumn(flags={'I18n'})

        by_standard = orb.Index(['standard'])

    templ = """\
    == MODEL ==
    {{ model.name }}

    == COLUMNS ==
    {% for column in add_columns %}
    {{ column.field }}
    {% endfor %}

    == I18N COLUMNS ==
    {% for column in add_i18n_columns %}
    {{ column.field }}
    {% endfor %}

    == INDEXES ==
    {% for index in add_indexes %}
    {{ index.name }}
    {% endfor %}
    """

    valid_empty_content = """
    == MODEL ==
    tests
    == COLUMNS ==
    == I18N COLUMNS ==
    == INDEXES =="""

    valid_full_content = """
    == MODEL ==
    tests
    == COLUMNS ==
    id
    standard
    == I18N COLUMNS ==
    translated
    == INDEXES ==
    tests_by_standard_idx
    """

    valid = mock_sql_conn(templates={
        'alter_table.sql.jinja': templ
    })

    empty_content, _ = valid.render_alter_table(Test)
    full_content, _ = valid.render_alter_table(Test, add={
        'fields': Test.schema().columns().values(),
        'indexes': Test.schema().indexes().values()
    })

    assert sql_equals(empty_content, valid_empty_content)
    assert sql_equals(full_content, valid_full_content)


def test_sql_connection_render_create_table(mock_sql_conn, sql_equals):
    import orb

    class Test(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        code = orb.StringColumn()
        display = orb.StringColumn(flags={'I18n'})
        parent = orb.ReferenceColumn('Test')
        parent_code = orb.StringColumn(shortcut='parent.code')

        by_code = orb.Index(['code'])


    templ = """\
    == MODEL ==
    {{ model.name }}
    id_column: {{ id_column.field }}

    == COLUMNS ==
    {% for column in add_columns %}
    {{ column.field }}
    {% endfor %}

    == I18N COLUMNS ==
    {% for column in add_i18n_columns %}
    {{ column.field }}
    {% endfor %}

    == INDEXES ==
    {% for index in add_indexes %}
    {{ index.name }}
    {% endfor %}
    """

    valid_full_content = """
    == MODEL ==
    tests
    id_column: id
    == COLUMNS ==
    code
    parent_id
    == I18N COLUMNS ==
    display
    == INDEXES ==
    """

    valid_reference_content = """
    == MODEL ==
    tests
    id_column: id
    == COLUMNS ==
    code
    == I18N COLUMNS ==
    display
    == INDEXES ==
    """

    valid = mock_sql_conn(templates={
        'create_table.sql.jinja': templ
    })

    full_content, _ = valid.render_create_table(Test)
    reference_content, _ = valid.render_create_table(Test, include_references=False)

    assert sql_equals(full_content, valid_full_content)
    assert sql_equals(reference_content, valid_reference_content)


def test_sql_connection_render_create_view(mock_sql_conn, sql_equals):
    import orb

    templ = """
    == MODEL ==
    {{ model.name }}
    id_column: {{ id_column.field }}
    == COLUMNS ==
    {% for column in add_columns %}
    {{ column.field }}
    {% endfor %}
    == I18N COLUMNS ==
    {% for column in add_i18n_columns %}
    {{ column.field }}
    {% endfor %}
    """

    system = orb.System()

    class Status(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        code = orb.StringColumn()
        display = orb.StringColumn(flags={'I18n'})

    class StatusView(orb.View):
        __system__ = system
        __id__ = 'status'

        status = orb.ReferenceColumn('User')
        status_code = orb.StringColumn(shortcut='status.code')
        status_display = orb.StringColumn(shortcut='status.display', flags={'I18n'})

    valid = mock_sql_conn(templates={
        'create_view.sql.jinja': templ
    })

    valid_full_content = """
    == MODEL ==
    status_views
    id_column: status_id
    == COLUMNS ==
    status_code
    == I18N COLUMNS ==
    status_display
    """

    full_content, _ = valid.render_create_view(StatusView)

    assert sql_equals(full_content, valid_full_content)


def test_sql_render_delete_collection(mock_sql_conn, sql_equals):
    import orb

    qtempl = """
    {{ query.column.field }} {{ query.op }} {{ query.value.variable }}
    """

    templ = """
    == MODEL ==
    {{ model.name }}
    == WHERE ==
    {{ where }}
    """

    system = orb.System()

    class Status(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        code = orb.StringColumn()
        display = orb.StringColumn(flags={'I18n'})

    valid = mock_sql_conn(templates={
        'delete.sql.jinja': templ,
        'query.sql.jinja': qtempl
    })

    valid_loaded_content = """
    == MODEL ==
    statuses
    == WHERE ==
    id IS IN (1,)
    """

    valid_where_content = """
    == MODEL ==
    statuses
    == WHERE ==
    code IS test
    """

    valid_all_content = """
    == MODEL ==
    statuses
    == WHERE ==
    """

    stat = Status({'id': 1})
    stat.mark_loaded()

    null_content, _ = valid.render_delete_collection(orb.Collection())
    loaded_content, _ = valid.render_delete_collection(orb.Collection([stat]))
    all_content, _ = valid.render_delete_collection(Status.all())
    where_content, _ = valid.render_delete_collection(Status.select(where=orb.Query('code') == 'test'))

    assert sql_equals(null_content, '')
    assert sql_equals(loaded_content, valid_loaded_content)
    assert sql_equals(where_content, valid_where_content)
    assert sql_equals(all_content, valid_all_content)


def test_sql_render_delete_records(mock_sql_conn, sql_equals):
    import orb

    qtempl = """
    {{ query.column.field }} {{ query.op }} {{ query.value.variable }}
    """

    templ = """
    == MODEL ==
    {{ model.name }}
    == WHERE ==
    {{ where }}
    """

    system = orb.System()

    class Status(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        code = orb.StringColumn()
        display = orb.StringColumn(flags={'I18n'})

    valid = mock_sql_conn(templates={
        'delete.sql.jinja': templ,
        'query.sql.jinja': qtempl
    })

    valid_loaded_content = """
    == MODEL ==
    statuses
    == WHERE ==
    id IS IN (1,)
    """

    stat = Status({'id': 1})
    stat.mark_loaded()

    null_content, _ = valid.render_delete_records([])
    loaded_content, _ = valid.render_delete_records([stat, Status()])

    assert sql_equals(null_content, '')
    assert sql_equals(loaded_content, valid_loaded_content)

import pytest


@pytest.fixture()
def mock_sql_conn(mock_db):
    from jinja2 import DictLoader
    from orb.testing import MockPooledConnectionMixin
    from orb.std.connections.sql.connection import SQLConnection

    def _mock_sql_conn(templates=None,
                       execute_response=None,
                       execute_native_response=None,
                       register_columns=True):
        import orb

        class MockSQLConnection(MockPooledConnectionMixin, SQLConnection):
            __templates__ = DictLoader(templates or {})

            def execute_native_command(self,
                                       conn,
                                       cmd,
                                       data=None,
                                       returning=None,
                                       mapper=None):
                if execute_native_response:
                    return execute_native_response(conn, cmd, data, returning, mapper)
                else:
                    return super(MockSQLConnection, self).execute_native_command(conn, cmd, data, returning, mapper)

            def execute(self, *args, **kw):
                if execute_response is not None:
                    return execute_response
                else:
                    return super(MockSQLConnection, self).execute(*args, **kw)

        # register types
        if register_columns:
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

def test_raise_error():
    import orb.errors
    from orb.std.connections.sql.connection import raise_error

    try:
        raise_error('OrbError', 'testing')
    except Exception as err:
        assert type(err) is orb.errors.OrbError
        assert str(err) == 'testing'
    else:
        assert False

    try:
        raise_error('UnknownError', 'testing')
    except Exception as err:
        assert type(err) is RuntimeError
        assert str(err) == 'testing'
    else:
        assert False


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


def test_sql_connection_insert(mock_sql_conn, mock_table):
    import orb

    empty = mock_sql_conn()
    valid = mock_sql_conn(templates={
        'insert.sql.jinja': ''
    })
    context = orb.Context()

    # validate an empty insertion
    records, count = valid.insert([], context)

    assert records == {}
    assert count == 0

    # validate a collection insertion
    a = mock_table({'username': 'john.smith'})
    records, count = valid.insert(orb.Collection([a]), context)
    assert records == {}
    assert count == 0

    # validate a records insertion
    records, count = valid.insert([a], context)
    assert records == {}
    assert count == 0

    with pytest.raises(Exception):
        empty.insert([a], context)


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
    value_h, _ = conn.process_value(column, orb.Query.Op.IsNotIn, [])

    with pytest.raises(orb.errors.QueryIsNull):
        conn.process_value(column, orb.Query.Op.IsIn, [])

    assert value_a == 'jdoe'
    assert value_b == 'jd%'
    assert value_c == 'jd%'
    assert value_d == '%oe'
    assert value_e == '%oe'
    assert value_f == '%do%'
    assert value_g == '%do%'
    assert value_h == ''


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
    {% for column in columns.add.standard %}
    {{ column.field }}
    {% endfor %}

    == I18N COLUMNS ==
    {% for column in columns.add.i18n %}
    {{ column.field }}
    {% endfor %}

    == INDEXES ==
    {% for index in indexes.add %}
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
    columns.id: {{ columns.id.field }}

    == COLUMNS ==
    {% for column in columns.standard %}
    {{ column.field }}
    {% endfor %}

    == I18N COLUMNS ==
    {% for column in columns.i18n %}
    {{ column.field }}
    {% endfor %}

    == INDEXES ==
    {% for index in indexes %}
    {{ index.name }}
    {% endfor %}
    """

    valid_full_content = """
    == MODEL ==
    tests
    columns.id: id
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
    columns.id: id
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
    columns.id: {{ columns.id.field }}
    == COLUMNS ==
    {% for column in columns.standard %}
    {{ column.field }}
    {% endfor %}
    == I18N COLUMNS ==
    {% for column in columns.i18n %}
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
    columns.id: status_id
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
    null_query_content, _ = valid.render_delete_collection(Status.select(where=orb.Query('code').in_([])))
    loaded_content, _ = valid.render_delete_collection(orb.Collection([stat]))
    all_content, _ = valid.render_delete_collection(Status.all())
    where_content, _ = valid.render_delete_collection(Status.select(where=orb.Query('code') == 'test'))

    assert sql_equals(null_content, '')
    assert sql_equals(null_query_content, '')
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


def test_sql_render_query_field(mock_sql_conn):
    import orb

    class Task(orb.Table):
        __register__ = False
        __namespace__ = 'public'

        id = orb.IdColumn()
        task_name = orb.StringColumn()

    task_name = Task.schema().column('task_name')
    q = orb.Query('task_name') == 'testing'
    conn = mock_sql_conn()

    test_field, test_data = conn.render_query_field(Task, task_name, q)
    assert test_field == '"public"."tasks"."task_name"'


def test_sql_render_query_field_with_alias(mock_sql_conn):
    import orb

    class Task(orb.Table):
        __register__ = False
        __namespace__ = 'public'

        id = orb.IdColumn()
        task_name = orb.StringColumn()

    task_name = Task.schema().column('task_name')
    q = orb.Query('task_name') == 'testing'
    conn = mock_sql_conn()

    test_field, test_data = conn.render_query_field(Task, task_name, q, aliases={Task: 't'})
    assert test_field == '"t"."task_name"'


def test_sql_render_query_field_with_functions(mock_sql_conn):
    import orb

    class Task(orb.Table):
        __register__ = False
        __namespace__ = 'public'

        id = orb.IdColumn()
        task_name = orb.StringColumn()

    task_name = Task.schema().column('task_name')
    q = orb.Query('task_name').lower() == 'testing'
    conn = mock_sql_conn()
    conn.register_function_mapping(orb.Query.Function.Lower, 'lower({0})')

    test_field, test_data = conn.render_query_field(Task, task_name, q)
    assert test_field == 'lower("public"."tasks"."task_name")'


def test_sql_render_query_field_with_basic_math(mock_sql_conn):
    import orb

    class Task(orb.Table):
        __register__ = False
        __namespace__ = 'public'

        id = orb.IdColumn()
        task_name = orb.StringColumn()

    task_name = Task.schema().column('task_name')
    q = (orb.Query('task_name') + 10) == 'testing'
    conn = mock_sql_conn()
    conn.register_math_mapping(orb.Query.Math.Add, '{0} + {1}')

    test_field, test_data = conn.render_query_field(Task, task_name, q)

    assert len(test_data) == 1
    assert test_data.values() == [10]
    assert (test_field % test_data) == '"public"."tasks"."task_name" + 10'


def test_sql_render_query_field_with_query_math(mock_sql_conn):
    import orb

    class Task(orb.Table):
        __register__ = False
        __namespace__ = 'public'

        id = orb.IdColumn()
        task_name = orb.StringColumn()

    task_name = Task.schema().column('task_name')
    q = (orb.Query('task_name') + orb.Query('task_name')) == 'testing'
    conn = mock_sql_conn()
    conn.register_math_mapping(orb.Query.Math.Add, '{0} + {1}')

    test_field, test_data = conn.render_query_field(Task, task_name, q)
    assert (test_field % test_data) == '"public"."tasks"."task_name" + "public"."tasks"."task_name"'


def test_sql_render_query_field_with_math_and_functions(mock_sql_conn):
    import orb

    class Task(orb.Table):
        __register__ = False
        __namespace__ = 'public'

        id = orb.IdColumn()
        task_name = orb.StringColumn()

    task_name = Task.schema().column('task_name')
    q = ((orb.Query('task_name') + orb.Query('task_name')) + 10).lower() == 'testing'

    conn = mock_sql_conn()
    conn.register_math_mapping(orb.Query.Math.Add, '{0} + {1}')
    conn.register_function_mapping(orb.Query.Function.Lower, 'lower({0})')

    test_field, test_data = conn.render_query_field(Task, task_name, q)
    sql = 'lower("public"."tasks"."task_name" + "public"."tasks"."task_name" + 10)'

    assert len(test_data) == 1
    assert test_data.values() == [10]
    assert (test_field % test_data) == sql


def test_sql_render_insert_collection(mock_sql_conn, mock_view, mock_table, sql_equals):
    import orb

    insert_templ = """
    {% set all_columns = columns.standard + columns.i18n %}
    == model ==
    {{ model.alias }}
    == schema ==
    {% for column in all_columns %}
    {{ column.field }}
    {% endfor %}
    == records ==
    {% for record in records %}
    ({% for column in all_columns %}{{ record[column.field].value }}{%- if not loop.last %}, {%- endif %}{% endfor %})
    {% endfor %}
    """

    class TestModel(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn()
        first_name = orb.StringColumn()
        last_name = orb.StringColumn(default='Doe')
        is_active = orb.BooleanColumn(default=True)
        title = orb.StringColumn(flags={'I18n'})

        @orb.virtual(orb.StringColumn)
        def display_name(self, **context):
            return '{} {}'.format(self.get('first_name'), self.get('last_name'))

        @display_name.setter()
        def set_display_name(self, value, **context):
            first_name, _, last_name = value.partition(' ')
            self.set('first_name', first_name)
            self.set('last_name', last_name)


    conn = mock_sql_conn(templates={
        'insert.sql.jinja': insert_templ
    })

    valid_cmd = """
    == model ==
    test_models

    == schema ==
    first_name
    id
    is_active
    last_name
    username
    title

    == records ==
    (None,None,True,Doe,john.doe,None)
    (None,None,True,Doe,jane.doe,None)
    """

    valid_second_cmd = """
    == model ==
    users

    == schema ==
    id
    username

    == records ==
    (None,john.smith)
    """

    a = TestModel({'username': 'john.doe'})
    b = TestModel({'username': 'jane.doe'})
    c = TestModel({'id': 1})
    c.mark_loaded()
    d = mock_table({'username': 'john.smith'})

    models = orb.Collection([a, c, b])

    test_empty, _ = conn.render_insert_collection(orb.Collection([], model=TestModel))
    test_cmd, _ = conn.render_insert_collection(models)
    test_records, _ = conn.render_insert_records([a, c, b, d])

    print(test_cmd)

    assert test_empty == ''
    assert sql_equals(test_cmd, valid_cmd)

    assert len(test_records) == 2
    assert sql_equals(test_records[0], valid_second_cmd) or sql_equals(test_records[0], valid_cmd)
    assert sql_equals(test_records[1], valid_second_cmd) or sql_equals(test_records[1], valid_cmd)

    # test invalid collection options
    with pytest.raises(orb.errors.QueryInvalid):
        conn.render_insert_collection(orb.Collection())

    with pytest.raises(orb.errors.QueryInvalid):
        conn.render_insert_collection(orb.Collection(model=mock_view))

    with pytest.raises(orb.errors.QueryInvalid):
        conn.render_insert_collection(orb.Collection(model=TestModel))


def test_render_query(mock_sql_conn, sql_equals):
    import orb

    q_templ = """
    {{ query.column.field }} {{ query.op }} {{ query.value.variable }}
    """
    qcompound_templ = """
    (
        {% for query in queries -%}
        {{ query }} {% if not loop.last -%} {{ op | upper }} {% endif -%}
        {% endfor %}
    )"""

    conn = mock_sql_conn(templates={
        'query.sql.jinja': q_templ,
        'query_compound.sql.jinja': qcompound_templ
    })

    class User(orb.Table):
        __register__ = False
        id = orb.IdColumn()
        username = orb.StringColumn()

    a = orb.Query('username') == 'test'
    b = orb.Query('username') == 'test2'

    valid_single = 'username IS test'
    valid_compound = '(username IS test OR username IS test2)'

    test_none, _ = conn.render_query(User, None)
    test_single, _ = conn.render_query(User, a)
    test_both, _ = conn.render_query(User, a | b)
    test_column, _ = conn.render_query_column(User, a)
    test_compound, _ = conn.render_query_compound(User, a | b)

    assert test_none == ''
    assert sql_equals(test_single, valid_single)
    assert sql_equals(test_both, valid_compound)
    assert test_single == test_column
    assert test_both == test_compound


def test_render_select(mock_sql_conn):
    import orb

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn()

    conn = mock_sql_conn(templates={
        'select.sql.jinja': ''
    })

    sql, _ = conn.render_select(User, orb.Context())
    assert sql == ''

    results, count = conn.select(User, orb.Context())
    assert results == {}
    assert count == 0


def test_render_update(mock_sql_conn, sql_equals):
    import orb

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn()
        first_name = orb.StringColumn()
        last_name = orb.StringColumn()

    update_templ = """
    update {{ model.name }} set
    {% for entry in changes.standard + changes.i18n %}
    {{ entry.column.field }} = {{ entry.value }}
    {% endfor %}
    where {{ id.field }} = {{ id.value }};
    """

    conn = mock_sql_conn(templates={
        'update.sql.jinja': update_templ
    })

    u = User({'id': 1, 'username': 'john.doe'})

    non_record_sql, _ = conn.render_update(u)
    assert non_record_sql == ''

    u.mark_loaded()

    non_changed_sql, _ = conn.render_update(u)
    assert non_changed_sql == ''

    u.set('username', 'jane.doe')

    valid_sql = """
    update users set
    username = jane.doe
    where id = 1;
    """

    test_sql, test_data = conn.render_update(u)
    assert sql_equals(test_sql, valid_sql)

    u.set('first_name', 'Jane')
    u.set('last_name', 'Doe')

    valid_sql = """
    update users set
    first_name = Jane
    last_name = Doe
    username = jane.doe
    where id = 1;
    """
    test_sql, test_data = conn.render_update(u, orb.Context())
    assert sql_equals(test_sql, valid_sql)


def test_render_update_with_i18n(mock_sql_conn, sql_equals):
    import orb

    class Article(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        code = orb.StringColumn()
        title = orb.StringColumn(flags={'I18n'})

    update_templ = """
    {% if changes.standard %}
    update {{ model.name }} set
    {% for entry in changes.standard %}
    {{ entry.column.field }} = {{ entry.value }}
    {% endfor %}
    where {{ id.field }} = {{ id.value }};
    {% endif %}

    {% if changes.i18n %}
    update {{ model.name }}_i18n set
    {% for entry in changes.i18n %}
    {{ entry.column.field }} = {{ entry.value }}
    {% endfor %}
    where {{ model.name }}_id = {{ id.value }} and locale = {{ locale }};
    {% endif %}
    """

    conn = mock_sql_conn(templates={
        'update.sql.jinja': update_templ
    })

    a = Article({'id': 1})
    a.mark_loaded()

    a.set('code', 'test_code')

    valid_sql = """
    update articles set
    code = test_code
    where id = 1;
    """

    test_sql, test_data = conn.render_update(a)
    assert sql_equals(test_sql, valid_sql)

    a.set('title', 'Test Title')

    valid_sql = """
    update articles set
    code = test_code
    where id = 1;

    update articles_i18n set
    title = Test Title
    where articles_id = 1 and locale = en_US;
    """
    test_sql, test_data = conn.render_update(a)
    assert sql_equals(test_sql, valid_sql)
    assert conn.update([a], orb.Context()) == ({}, 0)


def test_basic_get_column_type(mock_sql_conn):
    import orb

    conn = mock_sql_conn(register_columns=False)
    column = orb.StringColumn()

    # ensure we raise errors without a type mapping
    with pytest.raises(orb.errors.ColumnTypeNotFound):
        assert conn.get_column_type(column) is None

    # ensure we have a type mapping
    conn.register_type_mapping(orb.StringColumn, 'string')

    # ensure that the mapping works
    assert conn.get_column_type(column) == 'string'


def test_inherited_get_column_type(mock_sql_conn):
    import orb

    class CustomStringColumn(orb.StringColumn):
        pass

    conn = mock_sql_conn(register_columns=False)
    column = CustomStringColumn()

    # ensure we have a type mapping
    conn.register_type_mapping(orb.StringColumn, 'string')

    # ensure that the mapping works
    assert conn.get_column_type(column) == 'string'


def test_callable_column_type_mapping(mock_sql_conn):
    import orb

    conn = mock_sql_conn(register_columns=False)

    a = orb.StringColumn(max_length=5)
    b = orb.StringColumn(max_length=10)

    def render_string(column, context):
        return 'string({0})'.format(column.max_length())

    conn.register_type_mapping(orb.StringColumn, render_string)

    assert conn.get_column_type(a) == 'string(5)'
    assert conn.get_column_type(b) == 'string(10)'


def test_callable_column_type_mapping_with_inheritance(mock_sql_conn):
    import orb

    conn = mock_sql_conn(register_columns=False)

    class HexColumn(orb.StringColumn):
        pass

    class TextColumn(orb.StringColumn):
        pass

    a = orb.StringColumn(max_length=5)
    b = HexColumn(max_length=10)
    c = TextColumn(max_length=20)

    def render_string(column, context):
        return 'string({0})'.format(column.max_length())

    conn.register_type_mapping(orb.StringColumn, render_string)
    conn.register_type_mapping(TextColumn, 'text')

    assert conn.get_column_type(a) == 'string(5)'
    assert conn.get_column_type(b) == 'string(10)'
    assert conn.get_column_type(c) == 'text'

    assert conn.get_column_type(a) == 'string(5)'
    assert conn.get_column_type(b) == 'string(10)'


def test_default_namespace(mock_sql_conn):
    conn = mock_sql_conn()
    assert conn.get_default_namespace() == ''
    conn.__default_namespace__ = 'public'
    assert conn.get_default_namespace() == ''


def test_query_op(mock_sql_conn):
    import orb
    conn = mock_sql_conn()

    col = orb.StringColumn()

    assert conn.get_query_op(col, orb.Query.Op.IsNot) == 'IS NOT'
    assert conn.get_query_op(col, orb.Query.Op.Before) == 'BEFORE'
    assert conn.get_query_op(col, orb.Query.Op.After) == 'AFTER'

    def get_op(column, op, case_sensitive):
        assert column == col
        assert op == orb.Query.Op.After
        assert type(case_sensitive) is bool
        return '>'

    conn.register_query_op_mapping(orb.Query.Op.Before, '<')
    conn.register_query_op_mapping(orb.Query.Op.After, get_op)

    assert conn.get_query_op(col, orb.Query.Op.IsNot) == 'IS NOT'
    assert conn.get_query_op(col, orb.Query.Op.Before) == '<'
    assert conn.get_query_op(col, orb.Query.Op.After) == '>'

    with pytest.raises(KeyError):
        assert conn.get_query_op(col, -1)


def test_query_compound_op(mock_sql_conn):
    import orb
    conn = mock_sql_conn()

    assert conn.get_query_compound_op(orb.QueryCompound.Op.And) == 'AND'
    assert conn.get_query_compound_op(orb.QueryCompound.Op.Or) == 'OR'

    def get_op(op):
        assert op == orb.QueryCompound.Op.Or
        return '__or__'

    conn.register_query_compound_op_mapping(orb.QueryCompound.Op.And, '__and__')
    conn.register_query_compound_op_mapping(orb.QueryCompound.Op.Or, get_op)

    assert conn.get_query_compound_op(orb.QueryCompound.Op.And) == '__and__'
    assert conn.get_query_compound_op(orb.QueryCompound.Op.Or) == '__or__'

    with pytest.raises(KeyError):
        assert conn.get_query_compound_op(-1)


def test_wrap_query_function(mock_sql_conn):
    import orb

    conn = mock_sql_conn()
    conn.register_function_mapping(orb.Query.Function.AsString, '{0}::varchar')
    conn.register_function_mapping(orb.Query.Function.Lower, lambda col, field, op: 'lower({})'.format(field))

    column = orb.StringColumn(name='username')

    # without custom name
    assert conn.wrap_query_function(column, orb.Query.Function.Upper) == 'username'
    assert conn.wrap_query_function(column, orb.Query.Function.AsString) == 'username::varchar'
    assert conn.wrap_query_function(column, orb.Query.Function.Lower) == 'lower(username)'

    # with custom name
    assert conn.wrap_query_function(column, orb.Query.Function.Upper, field='test') == 'test'
    assert conn.wrap_query_function(column, orb.Query.Function.AsString, field='test') == 'test::varchar'
    assert conn.wrap_query_function(column, orb.Query.Function.Lower, field='test') == 'lower(test)'


def test_wrap_query_math(mock_sql_conn):
    import orb

    def mapping(col, field, op, value):
        return '{} - {}'.format(field, value), {}

    conn = mock_sql_conn()
    conn.register_math_mapping(orb.Query.Math.Add, '{0} + {1}')
    conn.register_math_mapping(orb.Query.Math.Subtract,
                               lambda col, field, op, value: '{} - {}'.format(field, value))

    column = orb.StringColumn(name='username')

    # without custom name
    assert conn.wrap_query_math(column, orb.Query.Math.Divide, 10) == 'username'
    assert conn.wrap_query_math(column, orb.Query.Math.Add, 10) == 'username + 10'
    assert conn.wrap_query_math(column, orb.Query.Math.Subtract, 10) == 'username - 10'

    # with custom name
    assert conn.wrap_query_math(column, orb.Query.Math.Divide, 10, field='test') == 'test'
    assert conn.wrap_query_math(column, orb.Query.Math.Add, 10, field='test') == 'test + 10'
    assert conn.wrap_query_math(column, orb.Query.Math.Subtract, 10, field='test') == 'test - 10'


def test_connection_execution(mock_sql_conn):
    import orb
    import time

    from orb.std.connections.sql.connection import SQLConnection

    class TestException(Exception):
        pass

    def run_command(conn, command, data, returning, mapper):
        if command == 'test command':
            return {'id': 1}, 1
        elif command == 'interrupt':
            raise orb.errors.Interruption
        elif command == 'raise_error':
            raise TestException
        elif command == 'sleep2':
            time.sleep(2)
            return {'id': 1}, 1
        elif command == 'sleep4':
            time.sleep(4)
            return {'id': 1}, 1
        elif command == 'sleep7':
            time.sleep(7)
            return {'id': 1}, 1


    conn = mock_sql_conn(execute_native_response=run_command)

    # assert valid inputs
    assert SQLConnection.execute(conn, '') == ({}, 0)
    assert SQLConnection.execute(conn, ['']) == ({}, 0)
    assert SQLConnection.execute(conn, ('',)) == ({}, 0)
    assert SQLConnection.execute(conn, {''}) == ({}, 0)

    with pytest.raises(RuntimeError):
        SQLConnection.execute(conn, 10)

    # test basic execution
    assert SQLConnection.execute(conn, ['test command']) == ({'id': 1}, 1)

    # test interrupted execution
    with pytest.raises(orb.errors.Interruption):
        SQLConnection.execute(conn, 'interrupt')

    # test errored execution
    with pytest.raises(TestException):
        SQLConnection.execute(conn, 'raise_error')

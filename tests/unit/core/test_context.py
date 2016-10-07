def _validate_context_dict(data):
    assert data['columns'] is None
    assert data['database'] is None
    assert data['distinct'] is False
    assert data['dryRun'] is False
    assert data['expand'] is None
    assert data['format'] == 'json'
    assert data['force'] is False
    assert data['inflated'] is None
    assert data['limit'] is None
    assert data['locale'] == 'en_US'
    assert data['namespace'] == ''
    assert data['force_namespace'] is False
    assert data['order'] is None
    assert data['page'] is None
    assert data['pageSize'] is None
    assert data['scope'] == {}
    assert data['returning'] == 'records'
    assert data['start'] is None
    assert data['timezone'] == 'US/Pacific'
    assert data['where'] is None
    assert data['useBaseQuery'] is True


def test_create_new_context():
    import pytest
    import orb.errors

    from orb.core.context import Context

    context = Context()

    assert context.columns is None
    assert context.database is None
    assert context.distinct is False
    assert context.dryRun is False
    assert context.expand is None
    assert context.format == 'json'
    assert context.force is False
    assert context.inflated is None
    assert context.limit is None
    assert context.locale == 'en_US'
    assert context.namespace == ''
    assert context.force_namespace is False
    assert context.order is None
    assert context.page is None
    assert context.pageSize is None
    assert context.scope == {}
    assert context.returning == 'records'
    assert context.start is None
    assert context.timezone == 'US/Pacific'
    assert context.where is None
    assert context.useBaseQuery is True

    # assert database doesn't have it
    with pytest.raises(orb.errors.DatabaseNotFound):
        context.db

    # assert that other properties raise an attribute error
    with pytest.raises(AttributeError):
        context.test_attribute


def test_context_attribute_setting():
    import orb
    import pytest

    from orb.core.context import Context

    context = Context()
    context.columns = ['username']
    context.database = 'orb_testing'
    context.distinct = True
    context.dryRun = True
    context.expand = 'user,group'
    context.format = 'text'
    context.force = True
    context.inflated = True
    context.limit = 10
    context.locale = 'fr_FR'
    context.namespace = 'public'
    context.force_namespace = True
    context.order = '-name'
    context.page = 2
    context.pageSize = 10
    context.scope = {'user': 'me'}
    context.returning = 'values'
    context.start = 10
    context.timezone = 'US/Eastern'
    context.where = orb.Query('id') == 5
    context.useBaseQuery = False

    with pytest.raises(AttributeError):
        context.test_attribute = 10

    assert context.columns == ['username']
    assert context.database == 'orb_testing'
    assert context.distinct is True
    assert context.dryRun is True
    assert context.expand == ['user', 'group']
    assert context.format == 'text'
    assert context.force is True
    assert context.inflated is True
    assert context.limit == 10
    assert context.locale == 'fr_FR'
    assert context.namespace == 'public'
    assert context.force_namespace == True
    assert context.order == [('name', 'desc')]
    assert context.page == 2
    assert context.pageSize == 10
    assert context.scope == {'user': 'me'}
    assert context.returning == 'values'
    assert context.start == 10
    assert context.timezone == 'US/Eastern'
    assert context.where.__json__() == (orb.Query('id') == 5).__json__()
    assert context.useBaseQuery is False


def test_context_hash_equivalency():
    from orb.core.context import Context

    a = Context()
    b = Context()

    assert hash(a) == hash(tuple(None for x in xrange(20)))
    assert a == b


def test_context_hash_equivalency_with_settings():
    from orb.core.context import Context

    a = Context(columns='username')
    b = Context(columns='username')
    c = Context(columns='user')

    assert hash(a) == hash(b)
    assert a == b
    assert a != c

    a = Context(scope={'custom_value': 10})
    b = Context()

    assert a == b


def test_context_hash_from_unhashable_value():
    from orb.core.context import Context

    a = Context(force_namespace={'test': 1})
    b = Context(force_namespace={'test': 1})

    assert a == b


def test_context_basic_hash_within_dictionary():
    from orb.core.context import Context

    a = Context()
    b = Context()

    check = {a: True}

    assert check.get(b) is True


def test_context_complex_hash_within_dictionary():
    from orb.core.context import Context

    a = Context(
        columns='username',
        distinct=True,
        order='+name',
        limit=10,
        start=10
    )
    b = Context(
        columns='username',
        distinct=True,
        order='+name',
        limit=10,
        start=10
    )

    check = {a: True}

    assert check.get(b) is True


def test_context_stack():
    import threading
    from orb.core.context import Context, _context_stack

    tid = threading.currentThread().ident

    assert len(_context_stack[tid]) == 0
    outer = Context(columns='username')
    with outer:
        assert len(_context_stack[tid]) == 1
        inner = Context()
    assert len(_context_stack[tid]) == 0

    assert outer == inner
    assert inner.columns == ['username']


def test_complex_context_stack():
    import threading
    from orb.core.context import Context, _context_stack

    tid = threading.currentThread().ident

    a = Context(scope={'user': 'me'})
    assert len(_context_stack[tid]) == 0
    with a:
        b = Context(columns='username,password')

        assert len(_context_stack[tid]) == 1
        with b:
            c = Context()

            assert len(_context_stack[tid]) == 2
            assert set(c.columns) == {'username', 'password'}
            assert c.scope == {'user': 'me'}

        assert len(_context_stack[tid]) == 1
    assert len(_context_stack[tid]) == 0


def test_context_conversion_to_dictionary():
    from orb.core.context import Context

    context = Context(db=None)
    data = dict(context)

    _validate_context_dict(data)


def test_context_duplication():
    from orb.core.context import Context

    a = Context(columns='username')
    b = a.copy()
    b.update({'columns': 'password'})

    assert set(a.columns) == {'username'}
    assert set(b.columns) == {'username', 'password'}


def test_custom_db_property():
    import orb
    import pytest
    from orb.core.context import Context

    a = Context()
    b = Context(db='custom')
    c = Context(database='testing')

    assert b.db == 'custom'

    # ensure default access to the db property
    # raises a database not found error
    with pytest.raises(orb.errors.DatabaseNotFound):
        a.db

    # ensure access to db when no registered database
    # is found raises an error
    with pytest.raises(orb.errors.DatabaseNotFound):
        c.db

    # ensure that if registering a database
    # it will now be found by the context
    db = orb.Database('SQLite', 'testing')
    orb.system.register(db)
    assert c.db == db
    orb.system.unregister(db)


def test_custom_expand_property():
    from orb.core.context import Context

    a = Context(expand='user,group')
    assert a.expand == ['user', 'group']

    b = Context(expand=['user', 'group'])
    assert b.expand == ['user', 'group']

    c = Context(expand={'user': {'username': {}, 'password': {}}})
    assert set(c.expand) == {'user', 'user.username', 'user.password'}

    d = Context(expand='user,user.username,user.password')
    assert d.expand == ['user', 'user.username', 'user.password']

    e = Context(expand={'user', 'group'})
    assert type(e.expand) == list
    assert set(e.expand) == {'user', 'group'}


def test_context_expand_tree():
    from orb.core.context import Context

    # expand no tree properties
    context = Context()
    tree = context.expandtree()
    assert tree == {}

    # expand column properties
    context = Context(expand='user,user.username,user.password,'
                             'group,group.name')
    tree = context.expandtree()

    assert sorted(tree.keys()) == ['group', 'user']
    assert sorted(tree['user'].keys()) == ['password', 'username']
    assert sorted(tree['group'].keys()) == ['name']


def test_context_expand_tree_with_model_defaults(MockUser):
    import orb
    from orb.core.context import Context

    context = Context()
    tree = context.expandtree(model=MockUser)

    assert tree.keys() == ['group']

def test_context_is_null():
    from orb.core.context import Context

    context = Context()
    assert context.isNull()

    context = Context(scope={})
    assert context.isNull()

    context = Context(columns='username')
    assert not context.isNull()

    context = Context(scope={'user': 'me'})
    assert not context.isNull()


def test_context_returns_items():
    from orb.core.context import Context

    context = Context()
    items = context.items()

    _validate_context_dict(dict(items))


def test_context_derives_locale_from_system_settings():
    import orb
    from orb.core.context import Context

    settings = orb.system.settings()
    base_locale = settings.default_locale

    try:
        a = Context()
        assert a.locale == settings.default_locale
        settings.default_locale = 'sp_SP'
        assert a.locale == 'sp_SP'
    finally:
        settings.default_locale = base_locale

    b = Context(locale='fr_FR')
    assert b.locale == 'fr_FR'


def test_collect_context_columns_from_schema(MockUser):
    import orb
    from orb.core.context import Context

    context = Context()
    assert context.schema_columns(MockUser.schema()) == []

    context = Context(columns='username')
    cols = context.schema_columns(MockUser.schema())
    assert cols[0] == MockUser.schema().column('username')


def test_custom_context_order_property():
    from orb.core.context import Context

    context = Context(order=[('name', 'asc')])
    assert context.order == [('name', 'asc')]

    context = Context(order='+first_name,-last_name')
    assert context.order == [('first_name', 'asc'), ('last_name', 'desc')]

    context = Context(order={('name', 'asc')})
    assert context.order == [('name', 'asc')]


def test_collect_context_returning_sub_context():
    import orb

    from orb.core.context import Context

    q = orb.Query('test') == True

    context = Context(where=q, scope={'user': 'me'})
    sub_context = context.sub_context()

    assert context.where.__json__() == q.__json__()
    assert sub_context.where is None
    assert context.scope['user'] == 'me'
    assert sub_context.scope['user'] == 'me'


def test_context_limit_custom_property():
    from orb.core.context import Context

    context = Context(limit=10)
    assert context.limit == 10
    assert context.pageSize is None

    context = Context(pageSize=10)
    assert context.limit == 10
    assert context.pageSize == 10

def test_context_custom_start_property():
    from orb.core.context import Context

    context = Context(start=10)
    assert context.page is None
    assert context.start == 10

    context = Context(page=2, pageSize=100)
    assert context.page == 2
    assert context.start == 100


def test_custom_timezone_property():
    import orb
    from orb.core.context import Context

    settings = orb.system.settings()
    base_timezone = settings.server_timezone

    context = Context()
    try:
        assert context.timezone == base_timezone
        settings.server_timezone = 'US/Eastern'
        assert context.timezone == 'US/Eastern'
    finally:
        settings.server_timezone = base_timezone

    context = Context(timezone='US/Central')
    assert context.timezone != settings.server_timezone
    assert context.timezone == 'US/Central'

def test_context_inheritance_with_scope_merge():
    from orb.core.context import Context

    context_a = Context(scope={'user': 'me'})
    context_b = Context(scope={'request': 1}, context=context_a)
    with context_a:
        context_c = Context(scope={'request': 2})

    assert context_a.scope == {'user': 'me'}
    assert context_b.scope == {'user': 'me', 'request': 1}
    assert context_c.scope == {'user': 'me', 'request': 2}


def test_context_inheritance_with_query_merge():
    import orb
    from orb.core.context import Context

    query_a = orb.Query('first_name') == 'John'
    query_b = orb.Query('last_name') == 'Doe'

    context_a = Context(where=query_a)
    context_b = Context(where=query_b, context=context_a)
    context_c = Context(where=query_b.__json__(), context=context_a)
    with context_a:
        context_d = Context(where=query_b)

    assert context_a.where.__json__() == query_a.__json__()
    assert context_b.where.__json__() == (query_a & query_b).__json__()
    assert context_c.where.__json__() == (query_a & query_b).__json__()
    assert context_d.where.__json__() == (query_a & query_b).__json__()


def test_context_inheritance_with_column_merge():
    from orb.core.context import Context

    cols_a = ['first_name']
    cols_b = ['last_name']

    context_a = Context(columns=cols_a)
    context_b = Context(columns=cols_b, context=context_a)
    with context_a:
        context_c = Context(columns=cols_b)

    assert context_a.columns == cols_a
    assert context_b.columns == (cols_a + cols_b)
    assert context_c.columns == (cols_a + cols_b)


def test_context_field_validation():
    import orb

    from orb.core.context import Context

    try:
        context = Context(limit=0)
    except orb.errors.ContextError as err:
        assert err.message == 'limit needs to be an integer greater than or equal to 1, got 0'

    try:
        context = Context(page=0)
    except orb.errors.ContextError as err:
        assert err.message == 'page needs to be an integer greater than or equal to 1, got 0'

    try:
        context = Context(pageSize=0)
    except orb.errors.ContextError as err:
        assert err.message == 'pageSize needs to be an integer greater than or equal to 1, got 0'

    try:
        context = Context(start=-1)
    except orb.errors.ContextError as err:
        assert err.message == 'start needs to be an integer greater than or equal to 0, got -1'

def test_context_update_with_null_value():
    from orb.core.context import Context

    a = Context()
    a.update(None)
    assert a.isNull()


def test_context_difference_checker():
    from orb.core.context import Context

    a = Context()
    b = Context(columns='id')
    c = Context(columns='id', start=1, limit=100)

    assert a.difference(a) == set()
    assert a.difference(b) == {'columns'}
    assert a.difference(c) == {'columns', 'start', 'limit'}
    assert b.difference(c) == {'start', 'limit'}
    assert c.difference(b) == {'start', 'limit'}
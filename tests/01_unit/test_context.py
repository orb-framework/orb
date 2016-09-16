import pytest

def test_context_null(orb):
    context = orb.Context()
    assert context.isNull() == True

@pytest.mark.run(order=1)
def test_context_no_database(orb):
    context = orb.Context()
    with pytest.raises(orb.errors.DatabaseNotFound):
        print context.db

def test_context(orb, User):
    with orb.Context(database='testing'):
        user_a = User()

    with orb.Context(database='testing_2'):
        user_b = User()

    assert user_a.context().database == 'testing'
    assert user_b.context().database == 'testing_2'

def test_invalid_context_properties(orb):
    with pytest.raises(orb.errors.InvalidContextOption):
        context = orb.Context(page=-1)

    with pytest.raises(orb.errors.InvalidContextOption):
        context = orb.Context(limit=-1)

    with pytest.raises(orb.errors.InvalidContextOption):
        context = orb.Context(pageSize=-1)

    with pytest.raises(orb.errors.InvalidContextOption):
        context = orb.Context(pageSize=0)

    with pytest.raises(orb.errors.InvalidContextOption):
        context = orb.Context(limit=0)

    with pytest.raises(orb.errors.InvalidContextOption):
        context = orb.Context(start=-1)

def test_valid_context_properties(orb):
    context = orb.Context(
        page=1,
        pageSize=10
    )

    assert context.page == 1
    assert context.pageSize == 10
    assert context.start == 0
    assert context.limit == 10

    context = orb.Context(
        page=2,
        pageSize=10
    )

    assert context.page == 2
    assert context.pageSize == 10
    assert context.start == 10
    assert context.limit == 10

def test_context_equality(orb):
    context_a = orb.Context(database='testing')
    context_b = orb.Context(database='testing2')
    context_c = orb.Context(database='testing')

    assert context_a != context_b
    assert context_a == context_c

def test_context_scope(orb, User):
    scope = {'session': 123}
    with orb.Context(scope=scope):
        user_a = User()

    assert user_a.context().scope == scope

def test_nested_context_scope(orb, User):
    scope_a = {'session': 123}
    scope_b = {'session': 234}

    with orb.Context(scope=scope_a) as context_a:
        print 'context a', context_a
        user_a = User()

        assert context_a.scope == scope_a
        assert user_a.context().scope == scope_a

        with orb.Context(scope=scope_b) as context_b:
            print 'context b', context_b

            context_c = orb.Context()
            user_b = User()

            print 'context c', context_c

            assert context_b.scope == scope_b
            assert context_c.scope == scope_b
            assert user_b.context().scope == scope_b

    assert user_a.context().scope == scope_a
    assert user_b.context().scope == scope_b

def test_locale_scope(orb, User):
    with orb.Context(locale='en_US'):
        user_a = User()
        user_b = User()

        with orb.Context(locale='fr_FR'):
            user_c = User()

    assert user_a.context().locale == 'en_US'
    assert user_b.context().locale == 'en_US'
    assert user_c.context().locale == 'fr_FR'

def test_context_update(orb):
    context_a = orb.Context()
    context_a.update({'limit': 1})
    assert context_a.limit == 1

def test_context_merge_by_dict(orb):
    context_a = orb.Context(limit=1)
    context_b = context_a.copy()
    context_b.update({'locale': 'fr_FR'})
    assert context_a.limit == 1
    assert context_b.limit == 1
    assert context_b.locale == 'fr_FR'

def test_context_merge_by_context(orb):
    context_a = orb.Context(limit=1)
    context_c = orb.Context(locale='fr_FR')
    context_b = context_a.copy()
    context_b.update(context_c)
    assert context_a.limit == 1
    assert context_b.limit == 1
    assert context_b.locale == 'fr_FR'

def test_context_hash(orb):
    context_a = orb.Context()
    context_b = orb.Context()

    assert hash(context_a) == hash(context_b)

    context_a = orb.Context(limit=10)
    context_b = orb.Context(context=context_a)

    assert hash(context_a) == hash(context_b)

    assert context_a == context_b

def test_context_dict_key(orb):
    context_a = orb.Context(limit=10)
    context_b = orb.Context(limit=10)

    test = {context_a: 1}

    assert test[context_b] == 1

def test_invalid_context_property(orb):
    context = orb.Context()
    assert context.inflated is None
    with pytest.raises(AttributeError):
        context.bad_property

    with pytest.raises(AttributeError):
        context.bad_property = 'bad'

def test_context_iterator(orb):
    context = orb.Context()
    for k, v in context:
        assert v == getattr(context, k)

def test_context_scope_copy(orb):
    context = orb.Context(scope={'request': 'blah'})
    context_b = context.copy()
    assert context.scope == context_b.scope

def test_context_expand_as_set(orb):
    context = orb.Context(expand={'user', 'group'})
    assert type(context.expand) == list
    assert 'user' in context.expand
    assert 'group' in context.expand

def test_context_expand_as_string(orb):
    context = orb.Context(expand='user,group')
    assert type(context.expand) == list
    assert 'user' in context.expand
    assert 'group' in context.expand

def test_context_order_as_string(orb):
    context = orb.Context(order='+id,-username')
    assert type(context.order) == list
    assert ('id', 'asc') in context.order
    assert ('username', 'desc') in context.order

def test_context_order_as_set(orb):
    context = orb.Context(order={('id', 'asc'), ('username', 'desc')})
    assert type(context.order) == list
    assert ('id', 'asc') in context.order
    assert ('username', 'desc') in context.order

def test_context_paging(orb):
    context = orb.Context(page=1, pageSize=10)
    assert context.start == 0
    assert context.limit == 10

    context = orb.Context(page=2, pageSize=10)
    assert context.start == 10
    assert context.limit == 10

    context = orb.Context(page=3, pageSize=10)
    assert context.start == 20
    assert context.limit == 10

def test_context_scope_merging(orb):
    with orb.Context(scope={'a': 10}) as a:
        with orb.Context(scope={'b': 20}) as b:
            assert a.scope.get('a') == 10
            assert a.scope.get('b') is None
            assert b.scope.get('a') == 10
            assert b.scope.get('b') == 20

        with orb.Context(scope={'a': 20}) as c:
            assert a.scope.get('a') == 10
            assert c.scope.get('a') == 20

def test_context_namespacing(orb):
    context = orb.Context(namespace='test')
    assert context.namespace == 'test'

    with orb.Context(namespace='test'):
        context = orb.Context()
        assert context.namespace == 'test'

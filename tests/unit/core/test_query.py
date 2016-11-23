import pytest


@pytest.fixture()
def mock_query_models():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    class B(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    return A, B


def test_query_state():
    from orb.core.query import (Query, State)

    text = 'UNDEFINED'

    assert Query.UNDEFINED.text == text
    assert str(Query.UNDEFINED) == 'Query.State({0})'.format(text)
    assert unicode(Query.UNDEFINED) == u'Query.State({0})'.format(text)
    assert Query.UNDEFINED == State(text)
    assert hash(Query.UNDEFINED) != hash(text)
    assert hash(Query.UNDEFINED) == hash((State, text))


def test_query_construction(mock_query_models):
    import orb

    A, B = mock_query_models

    # initialize a blank query
    q = orb.Query()
    assert q.is_null()

    # initialize a query with just a name
    q = orb.Query('name')
    assert q.model() is None
    assert q.model(A) is A
    assert q.model(B) is B
    assert q.column_name() == 'name'
    assert q.column() is None
    assert q.column(A) == A.schema().column('name')
    assert q.column(B) == B.schema().column('name')

    # initialize a query with a column instance
    q = orb.Query(A.schema().column('name'))
    assert q.model() is A
    assert q.column_name() == 'name'
    assert q.column() is A.schema().column('name')

    # initialize a query with a model and column as arguments
    q = orb.Query(A, 'name')
    assert q.model() is A
    assert q.model(B) is A
    assert q.column_name() == 'name'
    assert q.column() is A.schema().column('name')
    assert q.column(B) is A.schema().column('name')

    # initialize a query with a None model value
    q = orb.Query(None, 'name')
    assert q.model() is None
    assert q.model(B) is B
    assert q.column_name() == 'name'
    assert q.column() is None
    assert q.column(B) is B.schema().column('name')

    # initialize a query with a model and column as a tuple
    q = orb.Query((A, 'name'))
    assert q.model() is A
    assert q.model(B) is A
    assert q.column_name() == 'name'
    assert q.column() is A.schema().column('name')
    assert q.column(B) is A.schema().column('name')

    # initialize a query with a None model as a tuple
    q = orb.Query((None, 'name'))
    assert q.model() is None
    assert q.model(B) is B
    assert q.column_name() == 'name'
    assert q.column() is None
    assert q.column(B) is B.schema().column('name')

    # initialize a query with a model
    q = orb.Query(A)
    assert q.model() is A
    assert q.column_name() == 'id'
    assert q.column() == A.schema().id_column()

    # initialize with bad data
    with pytest.raises(RuntimeError):
        assert orb.Query(A, 'name', 'id') is None

    with pytest.raises(RuntimeError):
        assert orb.Query(orb.Column) is None

    with pytest.raises(RuntimeError):
        assert orb.Query(A()) is None


def test_query_construction_with_keywords():
    import orb

    q = orb.Query(
        'name',
        op='Is',
        value='testing',
        inverted=True,
        case_sensitive=True,
        functions=[orb.Query.Function.AsString, orb.Query.Function.Lower],
        math=[(orb.Query.Math.Add, 10)]
    )

    assert q.op() == orb.Query.Op.Is
    assert q.value() == 'testing'
    assert q.is_inverted() is True
    assert q.case_sensitive() is True
    assert q.functions() == [orb.Query.Function.AsString, orb.Query.Function.Lower]
    assert q.math() == [(orb.Query.Math.Add, 10)]

    with pytest.raises(RuntimeError):
        assert orb.Query('name', bad_param=None) is None


def test_query_hash_comparison():
    import orb

    a = orb.Query('name') == 'testing'
    b = orb.Query('name') == 'testing'
    c = orb.Query('name') != 'testing'
    d = orb.Query('name') == 'testing2'

    assert hash(a) == hash(b)
    assert hash(a) != hash(c)
    assert hash(a) != hash(d)


def test_query_hash_list_comparison():
    import orb

    a = orb.Query('name') == ['testing', 'other']
    b = orb.Query('name') == ['testing', 'other']
    c = orb.Query('name') != ['testing', 'other']
    d = orb.Query('name') == ['testing', 'others']

    assert hash(a) == hash(b)
    assert hash(a) != hash(c)
    assert hash(a) != hash(d)


def test_query_unhashable_item():
    import orb

    b = orb.Query('name').in_({'testing': 1})
    a = orb.Query('name').in_({'testing': 1})

    assert hash(a) == hash(b)


def test_query_emptiness():
    import orb

    a = orb.Query()
    b = orb.Query('id')
    c = orb.Query('id', op=orb.Query.Op.Is)
    d = orb.Query('id', value=10)
    e = orb.Query('id', op=orb.Query.Op.Is, value=10)

    for q in (a, b, c):
        assert bool(q) is False
        assert q.__nonzero__() is False
        assert q.__bool__() is False

    for q in (d, e):
        assert bool(q) is True
        assert q.__nonzero__() is True
        assert q.__bool__() is True


def test_query_column_inclusion(mock_query_models):
    import orb

    A, B = mock_query_models

    a = orb.Query('name')
    b = orb.Query(A, 'name')

    assert 'name' in a
    assert A.schema().column('name') in a
    assert B.schema().column('name') in a

    assert 'name' in b
    assert A.schema().column('name') in b
    assert B.schema().column('name') not in b


def test_query_addition_operator():
    import orb

    a = orb.Query('name')
    b = a + 'ing'

    assert a is not b
    assert a.math() == []
    assert b.column_name() == 'name'
    assert b.math() == [(orb.Query.Math.Add, 'ing')]


def test_query_absolute_operator():
    import orb

    a = orb.Query('offset')
    b = abs(a)

    assert a is not b
    assert a.functions() == []
    assert b.column_name() == 'offset'
    assert b.functions() == [orb.Query.Function.Abs]


def test_query_and_operator():
    import orb

    a = orb.Query('first_name') == 'john'
    b = orb.Query('last_name') == 'doe'

    c = a & None
    d = a & 10
    e = a & b

    assert a is not c
    assert hash(a) == hash(c)

    assert a is not d
    assert hash(a) != hash(d)
    assert d.math() == [(orb.Query.Math.And, 10)]

    assert a is not e
    assert hash(a) != hash(e)
    assert type(e) is orb.QueryCompound
    assert list(e) == [a, b]


def test_query_division_operator():
    import orb

    a = orb.Query('offset')
    b = a / 10

    assert a is not b
    assert a.math() == []
    assert b.column_name() == 'offset'
    assert b.math() == [(orb.Query.Math.Divide, 10)]


def test_query_equals_operator():
    import orb

    a = orb.Query('first_name') == 'jdoe'
    b = orb.Query('first_name').is_('jdoe')
    c = orb.Query('first_name')
    d = c == 'jdoe'

    assert a is not b
    assert c is not d
    assert hash(a) == hash(b) == hash(d) != hash(c)
    assert a.op() == orb.Query.Op.Is
    assert a.value() == 'jdoe'

def test_query_greater_than_operator():
    import orb

    a = orb.Query('value') > 10
    b = orb.Query('value').greater_than(10)
    c = orb.Query('value')
    d = c.greater_than(10)

    assert a is not b
    assert c is not d
    assert hash(a) == hash(b) == hash(d) != hash(c)
    assert a.op() == orb.Query.Op.GreaterThan
    assert a.value() == 10


def test_query_greater_than_or_equal_operator():
    import orb

    a = orb.Query('value') >= 10
    b = orb.Query('value').greater_than_or_equal(10)
    c = orb.Query('value')
    d = c.greater_than_or_equal(10)

    assert a is not b
    assert c is not d
    assert hash(a) == hash(b) == hash(d) != hash(c)
    assert a.op() == orb.Query.Op.GreaterThanOrEqual
    assert a.value() == 10


def test_query_less_than_operator():
    import orb

    a = orb.Query('value') < 10
    b = orb.Query('value').less_than(10)
    c = orb.Query('value')
    d = c.less_than(10)

    assert a is not b
    assert c is not d
    assert hash(a) == hash(b) == hash(d) != hash(c)
    assert a.op() == orb.Query.Op.LessThan
    assert a.value() == 10

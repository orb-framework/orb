import pytest


@pytest.fixture()
def mock_query_models():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()
        records = orb.ReverseLookup('B.a')

    class B(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()
        a = orb.ReferenceColumn('A')

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
    assert q.object_name() == 'name'
    assert q.column() is None
    assert q.column(A) == A.schema().column('name')
    assert q.column(B) == B.schema().column('name')

    # initialize a query with a column instance
    q = orb.Query(A.schema().column('name'))
    assert q.model() is A
    assert q.object_name() == 'name'
    assert q.column() is A.schema().column('name')
    assert q.collector() is None

    # initialize a query with a model and column as arguments
    q = orb.Query(A, 'name')
    assert q.model() is A
    assert q.model(B) is A
    assert q.object_name() == 'name'
    assert q.column() is A.schema().column('name')
    assert q.column(B) is A.schema().column('name')

    # initialize a query with a None model value
    q = orb.Query(None, 'name')
    assert q.model() is None
    assert q.model(B) is B
    assert q.object_name() == 'name'
    assert q.column() is None
    assert q.column(B) is B.schema().column('name')

    # initialize a query with a model and column as a tuple
    q = orb.Query((A, 'name'))
    assert q.model() is A
    assert q.model(B) is A
    assert q.object_name() == 'name'
    assert q.column() is A.schema().column('name')
    assert q.column(B) is A.schema().column('name')

    # initialize a query with a None model as a tuple
    q = orb.Query((None, 'name'))
    assert q.model() is None
    assert q.model(B) is B
    assert q.object_name() == 'name'
    assert q.column() is None
    assert q.column(B) is B.schema().column('name')

    # initialize a query with a model
    q = orb.Query(A)
    assert q.model() is A
    assert q.object_name() == 'id'
    assert q.column() == A.schema().id_column()
    
    # initialize a query with a collector string
    q = orb.Query('records')
    assert q.model() is None
    assert q.column(A) is None
    assert q.column(B) is None
    assert q.collector() is None
    assert q.collector(A) is A.schema().collector('records')
    assert q.collector(B) is None

    # initialize a query with a collector instance
    q = orb.Query(A.schema().collector('records'))
    assert q.object_name() == 'records'
    assert q.column() is None
    assert q.schema_object() is q.collector() is A.schema().collector('records')

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


def test_query_serialization():
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

    import pprint
    pprint.pprint(q.__json__())

    q_json = q.__json__()
    validate_json = {
        'type': 'query',
        'model': '',
        'column': 'name',
        'op': 'Is',
        'value': 'testing',
        'inverted': True,
        'case_sensitive': True,
        'functions': ['AsString', 'Lower'],
        'math': [
            {'op': 'Add', 'value': 10}
        ]
    }

    assert q_json == validate_json


def test_query_nested_serialization():
    import orb
    a = orb.Query('a')
    b = orb.Query('b')
    c = a == b

    validate_json = {
        'type': 'query',
        'model': '',
        'column': 'a',
        'op': 'Is',
        'value': {
            'type': 'query',
            'model': '',
            'column': 'b',
            'op': 'Is',
            'value': None,
            'inverted': False,
            'case_sensitive': False,
            'functions': [],
            'math': []
        },
        'inverted': False,
        'case_sensitive': False,
        'functions': [],
        'math': []
    }

    import pprint
    pprint.pprint(c.__json__())

    assert c.__json__() == validate_json


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
    c = orb.Query('name')
    c.append_math_op(orb.Query.Math.Add, 'ing')

    assert a is not b
    assert a.math() == []
    assert b.object_name() == 'name'
    assert b.math() == [(orb.Query.Math.Add, 'ing')]
    assert c.math() == [(orb.Query.Math.Add, 'ing')]
    assert hash(b) == hash(c) != hash(a)


def test_query_absolute_operator():
    import orb

    a = orb.Query('offset')
    b = abs(a)
    c = orb.Query('offset')
    c.append_function_op(orb.Query.Function.Abs)

    assert a is not b
    assert a.functions() == []
    assert b.object_name() == 'offset'
    assert b.functions() == [orb.Query.Function.Abs]
    assert c.functions() == [orb.Query.Function.Abs]
    assert hash(b) == hash(c)!= hash(a)


def test_query_and_operator():
    import orb

    a = orb.Query('first_name') == 'john'
    b = orb.Query('last_name') == 'doe'

    c = a & None
    d = a & 10
    e = a & b

    f = a.and_(None)
    g = a.and_(orb.Query())
    h = orb.Query().and_(a)

    assert a is not c
    assert hash(a) == hash(c)

    assert a is not d
    assert hash(a) != hash(d)
    assert d.math() == [(orb.Query.Math.And, 10)]

    assert a is not e
    assert hash(a) != hash(e)
    assert type(e) is orb.QueryCompound
    assert e.op() == orb.QueryCompound.Op.And
    assert list(e) == [a, b]

    # assert no changes when anding none values
    assert hash(f) == hash(a)
    assert hash(g) == hash(a)
    assert hash(h) == hash(a)


def test_query_division_operator():
    import orb

    a = orb.Query('offset')
    b = a / 10
    c = orb.Query('offset')
    c.append_math_op(orb.Query.Math.Divide, 10)

    assert a is not b
    assert a.math() == []
    assert b.object_name() == 'offset'
    assert b.math() == [(orb.Query.Math.Divide, 10)]
    assert c.math() == [(orb.Query.Math.Divide, 10)]
    assert hash(b) == hash(c) != hash(a)


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


def test_query_less_than_or_equal_operator():
    import orb

    a = orb.Query('value') <= 10
    b = orb.Query('value').less_than_or_equal(10)
    c = orb.Query('value')
    d = c.less_than_or_equal(10)

    assert a is not b
    assert c is not d
    assert hash(a) == hash(b) == hash(d) != hash(c)
    assert a.op() == orb.Query.Op.LessThanOrEqual
    assert a.value() == 10


def test_query_multiplication():
    import orb

    a = orb.Query('offset')
    b = a * 10
    c = orb.Query('offset')
    c.append_math_op(orb.Query.Math.Multiply, 10)

    assert a is not b
    assert a.math() == []
    assert b.object_name() == 'offset'
    assert b.math() == [(orb.Query.Math.Multiply, 10)]
    assert c.math() == [(orb.Query.Math.Multiply, 10)]
    assert hash(b) == hash(c) != hash(a)


def test_query_not_equals_operator():
    import orb

    a = orb.Query('first_name') != 'jdoe'
    b = orb.Query('first_name').is_not('jdoe')
    c = orb.Query('first_name')
    d = c != 'jdoe'

    assert a is not b
    assert c is not d
    assert hash(a) == hash(b) == hash(d) != hash(c)
    assert a.op() == orb.Query.Op.IsNot
    assert a.value() == 'jdoe'


def test_query_negation():
    import orb

    a = orb.Query('first_name') == 'jdoe'
    b = a.negated()

    assert a is not b
    assert hash(a) != hash(b)
    assert a.op() == orb.Query.Op.Is
    assert b.op() == orb.Query.Op.IsNot


def test_query_or_operator():
    import orb

    a = orb.Query('first_name') == 'john'
    b = orb.Query('last_name') == 'doe'

    c = a | None
    d = a | 10
    e = a | b

    f = a.or_(None)
    g = a.or_(orb.Query())
    h = orb.Query().or_(a)

    assert a is not c
    assert hash(a) == hash(c)

    assert a is not d
    assert hash(a) != hash(d)
    assert d.math() == [(orb.Query.Math.Or, 10)]

    assert a is not e
    assert hash(a) != hash(e)
    assert type(e) is orb.QueryCompound
    assert e.op() == orb.QueryCompound.Op.Or
    assert list(e) == [a, b]

    assert hash(f) == hash(a)
    assert hash(g) == hash(a)
    assert hash(h) == hash(a)


def test_query_subtraction():
    import orb

    a = orb.Query('offset')
    b = a - 10
    c = orb.Query('offset')
    c.append_math_op(orb.Query.Math.Subtract, 10)

    assert a is not b
    assert a.math() == []
    assert b.object_name() == 'offset'
    assert b.math() == [(orb.Query.Math.Subtract, 10)]
    assert c.math() == [(orb.Query.Math.Subtract, 10)]
    assert hash(b) == hash(c) != hash(a)


def test_query_as_string():
    import orb

    a = orb.Query('testing')
    b = a.as_string()

    assert a is not b
    assert hash(a) != hash(b)
    assert b.functions() == [orb.Query.Function.AsString]


def test_query_after_operator():
    import orb

    a = orb.Query('created_at')
    b = a.after('yesterday')

    assert a is not b
    assert hash(a) != hash(b)
    assert b.op() == orb.Query.Op.After
    assert b.value() == 'yesterday'


def test_query_before_operator():
    import orb

    a = orb.Query('created_at')
    b = a.before('now')

    assert a is not b
    assert hash(a) != hash(b)
    assert b.op() == orb.Query.Op.Before
    assert b.value() == 'now'


def test_query_between_operator():
    import orb

    a = orb.Query('threshold')
    b = a.between(1, 5)

    assert a is not b
    assert hash(a) != hash(b)
    assert b.op() == orb.Query.Op.Between
    assert b.value() == (1, 5)


def test_query_contains_operator():
    import orb

    a = orb.Query('name')
    b = a.contains('am')

    assert a is not b
    assert hash(a) != hash(b)
    assert b.op() == orb.Query.Op.Contains
    assert b.value() == 'am'


def test_query_does_not_contain_operator():
    import orb

    a = orb.Query('name')
    b = a.does_not_contain('am')

    assert a is not b
    assert hash(a) != hash(b)
    assert b.op() == orb.Query.Op.DoesNotContain
    assert b.value() == 'am'


def test_query_does_not_match_operator():
    import orb

    a = orb.Query('name')
    b = a.does_not_match('^\w+$')

    assert a is not b
    assert hash(a) != hash(b)
    assert b.op() == orb.Query.Op.DoesNotMatch
    assert b.value() == '^\w+$'


def test_query_endswith_operator(mock_db):
    import orb

    a = orb.Query('name')
    b = a.endswith('me')

    assert a is not b
    assert hash(a) != hash(b)
    assert b.op() == orb.Query.Op.Endswith
    assert b.value() == 'me'


def test_query_expansion(mock_db):
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

    valid_json = {
        'case_sensitive': False,
        'column': 'parent',
        'functions': [],
        'inverted': False,
        'math': [],
        'model': 'A',
        'op': 'IsIn',
        'type': 'query',
        'value': []
    }
    valid_value_json = {
        'case_sensitive': False,
        'column': 'name',
        'functions': [],
        'inverted': False,
        'math': [],
        'model': 'A',
        'op': 'Is',
        'type': 'query',
        'value': 'testing'
    }

    with orb.Context(db=mock_db()):
        a = orb.Query('parent.name') == 'testing'
        b = a.expand(model=A)
        test_json = b.__json__()
        test_value_json = b.value().context().where.__json__()

    import pprint
    pprint.pprint(test_json)
    pprint.pprint(test_value_json)

    assert hash(a) != hash(b)
    assert test_json == valid_json
    assert test_value_json == valid_value_json


def test_query_expansion_failure(mock_db):
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

    q = orb.Query('parent.name') == 'testing'
    with pytest.raises(orb.errors.QueryInvalid):
        assert q.expand() is None


def test_query_expansion_with_shortcut(mock_db):
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent_name = orb.StringColumn(shortcut='parent.name')
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

    valid_json = {
        'case_sensitive': False,
        'column': 'parent',
        'functions': [],
        'inverted': False,
        'math': [],
        'model': 'A',
        'op': 'IsIn',
        'type': 'query',
        'value': []
    }
    valid_value_json = {
        'case_sensitive': False,
        'column': 'name',
        'functions': [],
        'inverted': False,
        'math': [],
        'model': 'A',
        'op': 'Is',
        'type': 'query',
        'value': 'testing'
    }

    with orb.Context(db=mock_db()):
        a = orb.Query('parent_name') == 'testing'
        b = a.expand(model=A)
        test_json = b.__json__()
        test_value_json = b.value().context().where.__json__()

    import pprint
    pprint.pprint(test_json)
    pprint.pprint(test_value_json)

    assert hash(a) != hash(b)
    assert test_json == valid_json
    assert test_value_json == valid_value_json


def test_query_expansion_with_simple_column(mock_db):
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent_name = orb.StringColumn(shortcut='parent.name')
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

    valid_json = {
        'case_sensitive': False,
        'column': 'parent',
        'functions': [],
        'inverted': False,
        'math': [],
        'model': '',
        'op': 'Is',
        'type': 'query',
        'value': None
    }

    with orb.Context(db=mock_db()):
        a = orb.Query('parent') == None
        b = a.expand(model=A)
        test_json = b.__json__()

    import pprint
    pprint.pprint(test_json)

    assert hash(a) == hash(b)
    assert test_json == valid_json


def test_query_expansion_with_collector(mock_db):
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent_name = orb.StringColumn(shortcut='parent.name')
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

    valid_json = {
        'case_sensitive': False,
        'column': 'id',
        'functions': [],
        'inverted': False,
        'math': [],
        'model': 'A',
        'op': 'IsIn',
        'type': 'query',
        'value': []
    }

    with orb.Context(db=mock_db()):
        a = orb.Query('children.name') == 'testing'
        b = a.expand(model=A)
        test_json = b.__json__()

    import pprint
    pprint.pprint(test_json)

    assert hash(a) != hash(b)
    assert test_json == valid_json


def test_query_expansion_from_invalid_column(mock_db):
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent_name = orb.StringColumn(shortcut='parent.name')
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

    with orb.Context(db=mock_db()):
        a = orb.Query('name.name') == 'testing'
        with pytest.raises(orb.errors.QueryInvalid):
            assert a.expand(model=A) is None


def test_query_expansion_with_custom_filter(mock_db):
    import orb

    checks = {}

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent_name = orb.StringColumn(shortcut='parent.name')
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

        @parent.filter()
        def filter_parent(self, q, **context):
            if not 'filtered' in checks:
                checks['filtered'] = True
                return orb.Query('name') == 'testing'
            else:
                return orb.Query()

    valid_json = {
        'case_sensitive': False,
        'column': 'name',
        'functions': [],
        'inverted': False,
        'math': [],
        'model': '',
        'op': 'Is',
        'type': 'query',
        'value': 'testing'
    }

    with orb.Context(db=mock_db()):
        a = orb.Query('parent.name') == 'testing'
        b = a.expand(model=A)
        c = a.expand(model=A)
        test_json = b.__json__()

    import pprint
    pprint.pprint(test_json)

    assert hash(a) != hash(b) != hash(c)
    assert test_json == valid_json
    assert c.is_null()
    assert checks['filtered'] is True


def test_query_inversion():
    import orb

    a = orb.Query('name') == 'testing'
    b = a.inverted()

    assert hash(a) != hash(b)
    assert a.is_inverted() is False
    assert b.is_inverted() is True


def test_query_in_operator():
    import orb

    coll = orb.Collection()

    a = orb.Query('name').in_(['a', 'b'])
    b = orb.Query('name').in_(coll)
    c = orb.Query('name').in_('a')

    d = orb.Query('name').not_in(['a', 'b'])
    e = orb.Query('name').not_in(coll)
    f = orb.Query('name').not_in('a')

    assert a.value() == ('a', 'b')
    assert b.value() is coll
    assert c.value() == ('a',)

    assert d.value() == ('a', 'b')
    assert e.value() is coll
    assert f.value() == ('a',)


def test_query_lower_function():
    import orb

    a = orb.Query('name').lower()
    assert a.functions() == [orb.Query.Function.Lower]


def test_query_matches():
    import orb

    a = orb.Query('name').matches('^test$')
    assert a.op() == orb.Query.Op.Matches
    assert a.value() == '^test$'


def test_query_schema_objects():
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent_name = orb.StringColumn(shortcut='parent.name')
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

    name = A.schema().column('name')
    parent_name = A.schema().column('parent_name')

    a = orb.Query(name) == 'testing'
    b = orb.Query('name') == 'testing'
    c = orb.Query(name).in_([1, 2, 3])
    d = orb.Query(name) == orb.Query(parent_name)

    assert list(a.schema_objects()) == [name]
    assert list(b.schema_objects()) == []
    assert list(c.schema_objects()) == [name]
    assert list(d.schema_objects()) == [name, parent_name]


def test_query_startswith():
    import orb

    q = orb.Query('name').startswith('me')
    assert q.op() == orb.Query.Op.Startswith
    assert q.value() == 'me'


def test_query_upper_function():
    import orb

    q = orb.Query('name').upper()
    assert q.functions() == [orb.Query.Function.Upper]


def test_query_negated_ops():
    import orb

    Op = orb.Query.Op

    assert orb.Query.get_negated_op(Op.Is) == Op.IsNot
    assert orb.Query.get_negated_op(Op.IsNot) == Op.Is
    assert orb.Query.get_negated_op(Op.LessThan) == Op.GreaterThanOrEqual
    assert orb.Query.get_negated_op(Op.LessThanOrEqual) == Op.GreaterThan
    assert orb.Query.get_negated_op(Op.Before) == Op.After
    assert orb.Query.get_negated_op(Op.GreaterThan) == Op.LessThanOrEqual
    assert orb.Query.get_negated_op(Op.GreaterThanOrEqual) == Op.LessThan
    assert orb.Query.get_negated_op(Op.After) == Op.Before
    assert orb.Query.get_negated_op(Op.Contains) == Op.DoesNotContain
    assert orb.Query.get_negated_op(Op.DoesNotContain) == Op.Contains
    assert orb.Query.get_negated_op(Op.Startswith) == Op.DoesNotStartwith
    assert orb.Query.get_negated_op(Op.Endswith) == Op.DoesNotEndwith
    assert orb.Query.get_negated_op(Op.Matches) == Op.DoesNotMatch
    assert orb.Query.get_negated_op(Op.DoesNotMatch) == Op.Matches
    assert orb.Query.get_negated_op(Op.IsIn) == Op.IsNotIn
    assert orb.Query.get_negated_op(Op.IsNotIn) == Op.IsIn
    assert orb.Query.get_negated_op(Op.DoesNotStartwith) == Op.Startswith
    assert orb.Query.get_negated_op(Op.DoesNotEndwith) == Op.Endswith


def test_query_building():
    import orb

    q = orb.Query.build({'name': 'testing'})
    valid_json = {
        'case_sensitive': False,
        'column': 'name',
        'functions': [],
        'inverted': False,
        'math': [],
        'model': '',
        'op': 'Is',
        'type': 'query',
        'value': 'testing'
    }
    test_json = q.__json__()

    import pprint
    pprint.pprint(test_json)

    assert test_json == valid_json


def test_query_load_from_json():
    import orb

    test_json = {
        'case_sensitive': False,
        'column': 'name',
        'functions': [],
        'inverted': False,
        'math': [],
        'model': '',
        'op': 'Is',
        'type': 'query',
        'value': 'testing'
    }

    query = orb.Query.load(test_json)

    assert query.object_name() == 'name'
    assert query.op() == orb.Query.Op.Is
    assert query.value() == 'testing'


def test_query_load_from_json_as_compound():
    import orb

    test_json = {
        'type': 'compound',
        'op': 'Or',
        'queries': [{
            'case_sensitive': False,
            'column': 'name',
            'functions': [],
            'inverted': False,
            'math': [],
            'op': 'Is',
            'type': 'query',
            'value': 'testing'
        }, {
            'case_sensitive': False,
            'column': 'name',
            'functions': [],
            'inverted': False,
            'math': [],
            'model': '',
            'op': 'Is',
            'type': 'query',
            'value': 'test'
        }]
    }

    query = orb.Query.load(test_json)

    assert isinstance(query, orb.QueryCompound)
    assert query.op() == orb.QueryCompound.Op.Or
    assert query.at(0).object_name() == 'name'
    assert query.at(0).value() == 'testing'
    assert query.at(1).object_name() == 'name'
    assert query.at(1).value() == 'test'


def test_query_load_from_json_raises_model_not_found():
    import orb

    test_json = {
        'case_sensitive': False,
        'column': 'name',
        'functions': [],
        'inverted': False,
        'math': [],
        'model': 'A',
        'op': 'Is',
        'type': 'query',
        'value': 'testing'
    }

    system = orb.System()

    class A(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        name = orb.StringColumn()

    with pytest.raises(orb.errors.ModelNotFound):
        assert orb.Query.load(test_json) is None

    with orb.Context(system=system):
        q = orb.Query.load(test_json)

    assert q.model() == A
    assert q.schema_object() == A.schema().column('name')
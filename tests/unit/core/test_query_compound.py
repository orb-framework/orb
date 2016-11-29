import pytest


def test_query_compound_construction():
    import orb

    qc = orb.QueryCompound()
    assert qc.is_null() is True
    assert bool(qc) is False
    assert qc.__nonzero__() is False
    assert qc.__bool__() is False


def test_query_compound_hash_compare():
    import orb

    a = orb.Query('name') == 'testing'
    b = orb.Query('name') == 'testing'
    c = orb.QueryCompound(a, b)
    d = orb.QueryCompound(a, b)

    assert hash(c) == hash(d)


def test_query_compound_json_serialization():
    import orb

    a = orb.Query('name') == 'testing'
    b = orb.Query('name') == 'testing'
    c = orb.QueryCompound(a, b)

    test_json = c.__json__()

    import pprint
    pprint.pprint(test_json)

    valid_json = {
        'op': 'And',
        'queries': [{
            'case_sensitive': False,
            'column': 'name',
            'functions': [],
            'inverted': False,
            'math': [],
            'model': '',
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
            'value': 'testing'
        }],
        'type': 'compound'
    }

    assert test_json == valid_json


def test_query_compound_has_object():
    import orb

    a = orb.Query('name') == 'testing'
    b = orb.Query('parent_name') == 'testing'
    c = orb.QueryCompound(a, b)

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    assert 'name' in c
    assert 'parent_name' in c
    assert 'id' not in c
    assert A.schema().column('name') in c
    assert a in c


def test_query_compound_iteration():
    import orb

    a = orb.Query('name') == 'testing'
    b = orb.Query('parent_name') == 'testing'
    c = orb.QueryCompound(a, b)

    assert list(c) == [a, b]
    assert c.queries() == [a, b]


def test_query_compound_and_operator():
    import orb

    a = orb.Query('name') == 'testing'
    b = orb.Query('parent_name') == 'testing'
    c = orb.Query('last_name') == 'testing'

    d = orb.QueryCompound(a, b)
    e = d & c
    f = d & orb.Query()
    g = d.and_(c)
    h = d.and_(orb.Query())
    j = d.and_(None)
    k = orb.QueryCompound().and_(j)

    assert d.op() == orb.QueryCompound.Op.And
    assert e.op() == orb.QueryCompound.Op.And
    assert hash(d) != hash(e)
    assert hash(e) == hash(g)
    assert len(e) == 3
    assert len(d) == 2
    assert hash(f) == hash(h) == hash(j) == hash(k)


def test_query_compound_indexing():
    import orb

    a = orb.Query('name') == 'testing'
    b = orb.Query('parent_name') == 'testing'
    c = orb.QueryCompound(a, b)

    assert c[0] is a
    assert c[1] is b
    with pytest.raises(IndexError):
        assert c[2] is None


def test_query_compound_negation():
    import orb

    a = orb.Query('name') == 'testing'
    b = orb.Query('parent_name') == 'testing'
    c = orb.QueryCompound(a, b)
    d = c.negated()
    e = -c

    assert hash(d) == hash(e)
    assert d.op() is orb.QueryCompound.Op.Or
    assert e.op() is orb.QueryCompound.Op.Or


def test_query_compound_or_operator():
    import orb

    a = orb.Query('name') == 'testing'
    b = orb.Query('parent_name') == 'testing'
    c = orb.Query('last_name') == 'testing'

    d = orb.QueryCompound(a, b, op=orb.QueryCompound.Op.Or)
    e = d | c
    f = d | orb.Query()
    g = d.or_(c)
    h = d.or_(orb.Query())
    j = d.or_(None)
    k = orb.QueryCompound().or_(j)

    assert d.op() == orb.QueryCompound.Op.Or
    assert e.op() == orb.QueryCompound.Op.Or
    assert hash(d) != hash(e)
    assert hash(e) == hash(g)
    assert len(e) == 3
    assert len(d) == 2
    assert hash(f) == hash(h) == hash(j) == hash(k)


def test_query_compound_mix_and_match():
    import orb

    a = orb.Query('first_name') == 'john'
    b = orb.Query('first_name') == 'jane'
    c = orb.Query('last_name') == 'doe'

    d = (a & b) | c
    e = a & (b | c)
    f = a & b & c
    g = a | b | c
    h = (a | b) & c

    assert hash(d) != hash(e) != hash(f) != hash(g) != hash(h)

    assert len(d) == 2
    assert isinstance(d.at(0), orb.QueryCompound)
    assert isinstance(d.at(1), orb.Query)

    assert len(e) == 2
    assert isinstance(e.at(0), orb.Query)
    assert isinstance(e.at(1), orb.QueryCompound)

    assert len(h) == 2
    assert isinstance(h.at(0), orb.QueryCompound)
    assert isinstance(h.at(1), orb.Query)

    assert len(f) == 3
    assert isinstance(f.at(0), orb.Query)
    assert isinstance(f.at(1), orb.Query)
    assert isinstance(f.at(2), orb.Query)

    assert len(g) == 3
    assert isinstance(g.at(0), orb.Query)
    assert isinstance(g.at(1), orb.Query)
    assert isinstance(g.at(2), orb.Query)


def test_query_compound_at_indexing():
    import orb

    a = orb.Query('first_name') == 'john'
    b = orb.Query('first_name') == 'jane'
    c = orb.Query('last_name') == 'doe'

    d = (a & b) | c

    assert isinstance(d.at(0), orb.QueryCompound)
    assert isinstance(d.at(1), orb.Query)
    assert d.at(2) is None


def test_query_compound_models_and_objects():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    class B(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    class C(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a_name = A.schema().column('name')
    b_name = B.schema().column('name')
    c_name = C.schema().column('name')

    a = orb.Query(a_name) == 'testing'
    b = orb.Query(b_name) == 'testing'
    c = orb.Query(c_name) == 'testing'
    d = orb.QueryCompound(a, b)
    e = (a & b) | c

    assert list(d.models()) == [A, B]
    assert list(e.models()) == [A, B, C]
    assert list(d.schema_objects()) == [a_name, b_name]
    assert list(e.schema_objects()) == [a_name, b_name, c_name]


def test_query_compound_expand(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    class B(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        a = orb.ReferenceColumn('A')
        c = orb.ReferenceColumn('C')

    class C(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        b = orb.ReferenceColumn('B')
        d = orb.ReferenceColumn('D')

    class D(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    with orb.Context(db=mock_db()):
        q  = orb.Query(A) == orb.Query(B, 'a')
        q &= orb.Query(B, 'c') == orb.Query(C, 'b')
        q &= orb.Query(C, 'd') == orb.Query(D)
        q &= orb.Query(D, 'name') == 'testing'

        expanded = q.expand()
        test_json = expanded.__json__()

    import pprint
    pprint.pprint(test_json)

    valid_json = {
        'op': 'And',
        'queries': [{'case_sensitive': False,
                     'column': 'id',
                     'functions': [],
                     'inverted': False,
                     'math': [],
                     'model': 'A',
                     'op': 'IsIn',
                     'type': 'query',
                     'value': []}],
        'type': 'compound'
    }

    assert valid_json == test_json
import pytest


def test_blank_index_creation():
    from orb.core.index import Index

    index = Index()

    assert index.name() == ''
    assert index.columns() == []
    assert index.dbname() == ''
    assert index.flags() == 0
    assert index.order() is None
    assert index.schema() is None


def test_index_creation_with_properties():
    from orb.core.index import Index

    index = Index(
        columns=['username'],
        name='by_username',
        dbname='by_username_idx',
        flags={'Unique'}
    )

    assert index.name() == 'by_username'
    assert index.columns() == ['username']
    assert index.dbname() == 'by_username_idx'
    assert index.flags() == Index.Flags.Unique
    assert index.order() is None
    assert index.schema() is None


def test_index_creation_with_schema(MockUser):
    from orb.core.index import Index

    index = Index(
        columns=['username'],
        name='by_username',
        flags={'Unique'},
        schema=MockUser.schema()
    )

    assert index.name() == 'by_username'
    assert index.__name__ == 'by_username'
    assert index.columns() == ['username']
    assert index.dbname() == 'mock_users_by_username_idx'
    assert index.flags() == Index.Flags.Unique
    assert index.order() is None
    assert index.schema() is MockUser.schema()


def test_index_setters():
    from orb.core.index import Index

    a = Index()
    a.set_name('testing')
    a.set_columns(['username'])
    a.set_dbname('testing_idx')
    a.set_order('+id')
    a.set_flags(Index.Flags.Unique)

    assert a.name() == 'testing'
    assert a.columns() == ['username']
    assert a.dbname() == 'testing_idx'
    assert a.order() == '+id'
    assert a.flags() == Index.Flags.Unique


def test_index_comparisons():
    from orb.core.index import Index

    a = Index(name='a')
    b = Index(name='b')
    c = Index(name='c')

    unordered = [b, c, a]
    ordered = list(sorted(unordered))

    assert ordered == [a, b, c]
    assert a == a
    assert a != b
    assert a < 1


def test_index_query_building(MockUser, MockGroup):
    import orb
    from orb.core.index import Index

    a = Index(['username', 'first_name'], schema=MockUser.schema())
    b = Index(['username'], schema=MockUser.schema())
    c = Index(['username'])

    qa = a.build_query(('jdoe', 'John'))
    assert isinstance(qa, orb.QueryCompound)
    assert qa.at(0).column_name() == 'username'
    assert qa.at(1).column_name() == 'first_name'

    qb = b.build_query(('jdoe',))
    assert isinstance(qb, orb.Query)

    qc = c.build_query(('jdoe',), schema=MockUser.schema())
    assert isinstance(qc, orb.Query)

    with pytest.raises(orb.errors.OrbError):
        assert c.build_query(('jdoe',))

    with pytest.raises(orb.errors.ColumnNotFound):
        assert c.build_query(('jdoe',), schema=MockGroup.schema())

    with pytest.raises(TypeError):
        assert a.build_query(('jdoe',))


def test_index_copying(MockUser):
    from orb.core.index import Index

    a = Index(
        name='testing',
        columns=['a', 'b'],
        schema=MockUser.schema()
    )
    b = a.copy()

    assert a.columns() == b.columns()
    assert a.columns() is not b.columns()
    assert a.name() == b.name()
    assert a.schema() is not b.schema()


def test_index_flags():
    from orb.core.index import Index

    a = Index(flags={'Unique', 'Static'})
    assert a.flags() == (Index.Flags.Unique | Index.Flags.Static)
    assert a.test_flag(Index.Flags.Unique)
    assert not a.test_flag(Index.Flags.Virtual)
    a.set_flags(Index.Flags.Static)
    assert not a.test_flag(Index.Flags.Unique)


def test_column_validation(MockUser, MockGroup):
    import orb
    from orb.core.index import Index

    a = Index(
        columns=['username', 'first_name'],
        schema=MockUser.schema()
    )

    user = MockUser()
    group = MockGroup()

    values = {
        user.schema().column('username'): 'jdoe',
        user.schema().column('first_name'): 'John'
    }
    assert a.validate(user, values)
    with pytest.raises(orb.errors.InvalidIndexArguments):
        assert a.validate(group, values)

    values = {
        'username': 'jdoe',
        'first_name': 'John'
    }
    with pytest.raises(orb.errors.InvalidIndexArguments):
        assert a.validate(user, values)


def test_index_callable(MockUser, MockGroup, mock_db):
    import orb
    from orb.core.index import Index

    db = mock_db()

    by_username = Index(['username'], order='-id')
    results = by_username(MockUser, 'jdoe')

    assert isinstance(results, orb.Collection)

    with orb.Context(db=db):
        by_username.set_flags(Index.Flags.Unique)
        assert by_username(MockUser, 'jdoe') is None

    by_username.set_schema(MockUser.schema())
    with pytest.raises(orb.errors.OrbError):
        assert by_username(MockGroup, 'jdoe')



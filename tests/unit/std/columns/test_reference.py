import pytest


def test_reference_column_with_model_definition():
    import orb

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    col = orb.ReferenceColumn(User)
    assert col.reference() == 'User'
    assert col.reference_model() == User
    assert col.reference_column() == User.schema().id_column()


def test_reference_column_aliasing():
    import orb

    a = orb.ReferenceColumn(name='user')
    b = orb.ReferenceColumn(name='user', field='fkey_users__user')
    c = orb.ReferenceColumn(name='user',
                            field='fkey_users__user',
                            alias='user_fkey')
    d = orb.ReferenceColumn(name='user',
                            alias='user_fkey')

    assert a.name() == 'user'
    assert a.alias() == 'user_id'
    assert a.field() == 'user_id'

    assert b.name() == 'user'
    assert b.alias() == 'user_id'
    assert b.field() == 'fkey_users__user'

    assert c.name() == 'user'
    assert c.alias() == 'user_fkey'
    assert c.field() == 'fkey_users__user'

    assert d.name() == 'user'
    assert d.alias() == 'user_fkey'
    assert d.field() == 'user_fkey'


def test_reference_column_with_model_name():
    import orb

    system = orb.System()

    class User(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        parent = orb.ReferenceColumn('User')

    col = User.schema().column('parent')
    assert col.reference() == 'User'
    assert col.reference_model() == User
    assert col.reference_column() == User.schema().id_column()


def test_reference_column_with_custom_column():
    import orb

    system = orb.System()

    class User(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        guid = orb.StringColumn()
        parent = orb.ReferenceColumn('User', 'guid')

    col = User.schema().column('parent')
    assert col.reference() == 'User'
    assert col.reference_model() == User
    assert col.reference_column() == User.schema().column('guid')


def test_reference_column_with_missing_reference():
    import orb

    system = orb.System()

    class User(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        role = orb.ReferenceColumn('Role')

    col = User.schema().column('role')
    with pytest.raises(orb.errors.ModelNotFound):
        assert col.reference_model() is None


def test_reference_column_copy():
    import orb

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn()

    col = orb.ReferenceColumn(User, 'username', remove_action=orb.ReferenceColumn.RemoveAction.Cascade)
    col_b = col.copy()

    assert col.reference_model() == User
    assert col_b.reference_model() == User

    assert col.reference_column() == User.schema().column('username')
    assert col_b.reference_column() == User.schema().column('username')

    assert col.remove_action() == orb.ReferenceColumn.RemoveAction.Cascade
    assert col_b.remove_action() == orb.ReferenceColumn.RemoveAction.Cascade

    col.set_remove_action(orb.ReferenceColumn.RemoveAction.Block)
    assert col.remove_action() != col_b.remove_action()


def test_reference_column_restore():
    import orb

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn()

    col_a = orb.ReferenceColumn(User)
    col_b = orb.ReferenceColumn(User, name='created_by', flags={'I18n'})

    a = col_a.restore(1, orb.Context(returning='values'))
    b = col_a.restore({'id': 1, 'username': 'jdoe'})
    c = col_a.restore('{"id": 1, "username": "jdoe"}')
    d = col_b.restore({'en_US': {'id': 1}, 'fr_FR': {'id': 2}})
    e = col_b.restore('{"en_US": {"id": 1}, "fr_FR": {"id": 2}}')
    f = col_b.restore({'en_US': {'id': 1}, 'fr_FR': {'id': 2}}, orb.Context(locale='all'))

    assert a == 1

    assert isinstance(b, User)
    assert b.get('id') == 1
    assert b.get('username') == 'jdoe'

    assert isinstance(c, User)
    assert c.get('id') == 1
    assert c.get('username') == 'jdoe'

    assert isinstance(d, User)
    assert d.get('id') == 1

    assert isinstance(e, User)
    assert e.get('id') == 1

    assert type(f) == dict
    assert set(f.keys()) == {'en_US', 'fr_FR'}
    assert f['en_US'].get('id') == 1
    assert f['fr_FR'].get('id') == 2


def test_reference_column_validation():
    import orb

    system = orb.System()

    class User(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        username = orb.StringColumn()

    class Employee(User):
        __system__ = system

        first_name = orb.StringColumn()
        last_name = orb.StringColumn()

    class Role(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        name = orb.StringColumn()

    col = orb.ReferenceColumn(User)

    assert col.validate(1)
    assert col.validate(User({'id': 1}))
    assert col.validate(Employee({'id': 1}))

    with pytest.raises(orb.errors.InvalidReference):
        assert col.validate(Role({'id': 1})) is None


def test_reference_column_value_from_string(mock_db):
    import orb

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()


    col = orb.ReferenceColumn(User)

    assert col.value_from_string(None) is None

    responses = {
        'select': ({'id': 1, 'name': 'jdoe'},)
    }

    with orb.Context(db=mock_db(responses=responses)):
        a = col.value_from_string('jdoe')
        b = col.value_from_string('{"id": 1, "name": "testing"}')

    assert a.get('id') == 1
    assert a.get('name') == 'jdoe'
    assert b.get('id') == 1
    assert b.get('name') == 'testing'
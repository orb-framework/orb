import pytest


@pytest.fixture()
def UserSchema():
    from orb.core.schema import Schema
    from orb.core.system import System
    from orb.core.column import Column
    from orb.core.collector import Collector
    from orb.core.index import Index

    system = System()

    schema = Schema(
        name='User',
        system=system,
        columns=[
            Column(name='id'),
            Column(name='username'),
            Column(name='permits', flags={'Private'})
        ],
        indexes=[
            Index(name='by_username'),
            Index(name='by_permits', flags={'Private'})
        ],
        collectors=[
            Collector(name='groups'),
            Collector(name='permissions', flags={'Private'})
        ]
    )

    system.register(schema)

    return schema


def test_basic_schema_creation():
    import orb
    from orb.core.schema import Schema

    schema = Schema()
    assert schema.name() == ''
    assert schema.display() == ''
    assert schema.group() == ''
    assert schema.inherits() == ''
    assert schema.flags() == 0
    assert schema.alias() == ''
    assert schema.dbname() == ''
    assert schema.namespace() == ''
    assert schema.database() == ''
    assert schema.columns() == {}
    assert schema.indexes() == {}
    assert schema.collectors() == {}
    assert schema.system() == orb.system

    with pytest.raises(orb.errors.ColumnNotFound):
        assert schema.id_column() is None


def test_basic_schema_creation_with_properties():
    from orb.core.schema import Schema

    schema = Schema(
        name='MockUser',
        group='auth',
        inherits='MockAccount',
    )

    assert schema.name() == 'MockUser'
    assert schema.display() == 'Mock User'
    assert schema.inherits() == 'MockAccount'
    assert schema.group() == 'auth'
    assert schema.alias() == 'mock_users'
    assert schema.dbname() == 'mock_users'


def test_basic_schema_creation_with_overrides():
    from orb.core.schema import Schema

    schema = Schema(
        name='MockUser',
        alias='mock_user',
        dbname='users',
        display='User'
    )

    assert schema.name() == 'MockUser'
    assert schema.display() == 'User'
    assert schema.alias() == 'mock_user'
    assert schema.dbname() == 'users'


def test_schema_ancestry():
    from orb.core.system import System
    from orb.core.schema import Schema

    system = System()

    a = Schema(name='Fruit', system=system)
    b = Schema(name='Apple', inherits='Fruit', system=system)
    c = Schema(name='GrannySmith', inherits='Apple', system=system)

    system.register(a)
    system.register(b)
    system.register(c)

    a_inherits = list(a.ancestry())
    b_inherits = list(b.ancestry())
    c_inherits = list(c.ancestry())

    assert a_inherits == []
    assert b_inherits == [a]
    assert c_inherits == [b, a]


def test_schema_ordering():
    from orb.core.system import System
    from orb.core.schema import Schema

    system = System()

    a = Schema(name='Fruit', system=system)
    b = Schema(name='Apple', inherits='Fruit', system=system)
    c = Schema(name='GrannySmith', inherits='Apple', system=system)
    d = Schema(name='Vegetable', system=system)
    e = Schema(name='Broccoli', inherits='Vegetable', system=system)

    system.register(a)
    system.register(b)
    system.register(c)
    system.register(d)
    system.register(e)

    schemas = [a, b, c, d, e, 1]
    schemas.sort()

    assert schemas == [a, d, b, e, c, 1]


def test_schema_serialization(UserSchema):
    data = UserSchema.__json__()

    assert data['model'] == 'User'
    assert data['id_column'] == 'id'
    assert data['dbname'] == 'users'
    assert data['display'] == 'User'
    assert data['inherits'] == ''
    assert data['flags'] == {}
    assert len(data['columns']) == 2
    assert len(data['indexes']) == 1
    assert len(data['collectors']) == 1


def test_schema_with_inheritance(UserSchema):
    import orb
    from orb.core.schema import Schema
    from orb.core.column import Column

    schema = Schema(
        name='Employee',
        inherits='User',
        columns=[
            Column(name='role')
        ],
        system=UserSchema.system()
    )

    UserSchema.system().register(schema)

    assert schema.column('role').schema() == schema
    assert schema.column('username').schema() == UserSchema
    assert schema.index('by_username').schema() == UserSchema
    assert schema.collector('groups').schema() == UserSchema

    with pytest.raises(orb.errors.ColumnNotFound):
        assert not schema.column('role_name')

    assert schema.column('role_name', raise_=False) is None


def test_schema_column_association(UserSchema):
    from orb.core.column import Column

    a = Column()
    b = Column()

    assert a != b
    assert a not in [b]

    assert UserSchema.has_column('username')
    assert UserSchema.has_column(UserSchema.column('username'))
    assert UserSchema.has_column(Column()) is False


def test_schema_with_translation_columns(UserSchema):
    from orb.core.schema import Schema
    from orb.core.column import Column

    schema = Schema(
        name='testing',
        columns=[
            Column(name='title', flags={'I18n'})
        ]
    )

    assert UserSchema.has_translations() is False
    assert schema.has_translations() is True


def test_schema_with_namespace():
    from orb.core.schema import Schema

    a = Schema(namespace='groups')
    b = Schema()

    assert a.namespace() == 'groups'
    assert b.namespace() == ''
    assert a.namespace(namespace='testing') == 'groups'
    assert b.namespace(namespace='testing') == 'testing'
    assert a.namespace(namespace='testing', force_namespace=True) == 'testing'
    assert b.namespace(namespace='testing', force_namespace=True) == 'testing'


def test_schema_registration():
    from orb.core.schema import Schema
    from orb.core.column import Column
    from orb.core.collector import Collector
    from orb.core.index import Index

    a = Schema()
    a.register(Column(name='id'))
    a.register(Collector(name='groups'))
    a.register(Index(name='by_group'))

    def testing():
        pass

    setattr(testing, '__orb__', Column(name='testing'))
    a.register(testing)

    with pytest.raises(RuntimeError):
        a.register(Schema(name='schema'))

    assert a.column('id') is not None
    assert a.collector('groups') is not None
    assert a.index('by_group') is not None
    assert a.column('testing') is not None


def test_schema_flags():
    from orb.core.schema import Schema

    schema = Schema(flags={'Private'})
    assert schema.test_flag(Schema.Flags.Private)
    assert schema.test_flag(Schema.Flags.Abstract) is False


def test_schema_does_not_serialize_private_relations():
    import orb
    from orb.core.system import System

    system = System()

    class User(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        group = orb.ReferenceColumn('Group', field='group')

        groups = orb.Collector(model='Group')

    class Group(orb.Table):
        __system__ = system
        __flags__ = {'Private'}

        id = orb.IdColumn()
        name = orb.StringColumn()

    jdata = User.schema().__json__()

    assert 'group' not in jdata['columns']
    assert 'groups' not in jdata['collectors']


def test_schema_registration_of_index_creates_method():
    import orb
    from orb.core.system import System
    from orb.core.index import Index

    system = System()

    class User(orb.Table):
        __system__ = system

    User.schema().register(Index(name='by_username'))
    assert User.by_username is not None


def test_access_of_column_via_subpath():
    import orb

    system = orb.System()

    class User(orb.Table):
        __system__ = system

        group = orb.ReferenceColumn('Group')

    class Group(orb.Table):
        __system__ = system

        role = orb.ReferenceColumn('Role')

    class Role(orb.Table):
        __system__ = system

        name = orb.StringColumn()

    assert system.model('User') == User
    assert system.model('Group') == Group
    assert system.model('Role') == Role
    assert User.schema().column('group').schema().system() == system
    assert User.schema().column('group').reference_model() == Group
    assert User.schema().column('group.role.name') == Role.schema().column('name')

    with pytest.raises(orb.errors.ColumnNotFound):
        assert not User.schema().column('group.role.person')

    with pytest.raises(orb.errors.ColumnNotFound):
        assert not User.schema().column('group.missing')

    assert User.schema().column('group.role.missing', raise_=False) is None
    assert User.schema().column('group.code', raise_=False) is None
    assert User.schema().column('group.code', raise_=False) is None
    assert User.schema().column('group.role.missing', raise_=False) is None
    assert User.schema().column('group.name.missing', raise_=False) is None

    with pytest.raises(orb.errors.ColumnNotFound):
        assert not User.schema().column('group.role.missing')

    with pytest.raises(orb.errors.ColumnNotFound):
        assert not User.schema().column('group.name.missing_also')
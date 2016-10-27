import pytest


def test_create_new_system():
    from orb.core.system import System

    system = System()
    assert system.database() is None
    assert system.schemas() == {}

    assert system.settings.default_locale == 'en_US'
    assert system.settings.server_timezone == 'US/Pacific'
    assert system.settings.max_connections == 10


def test_create_new_system_with_custom_settings():
    from orb.core.system import System

    system = System(default_locale='fr_FR', max_connections=20)
    assert system.database() is None
    assert system.schemas() == {}

    assert system.settings.default_locale == 'fr_FR'
    assert system.settings.server_timezone == 'US/Pacific'
    assert system.settings.max_connections == 20


def test_create_and_activate_database():
    from orb.testing import MockConnection
    from orb.core.database import Database
    from orb.core.system import System

    db = Database(MockConnection())
    system = System()

    system.activate(db)

    assert system.database() == db


def test_create_and_register_database():
    from orb import errors
    from orb.testing import MockConnection
    from orb.core.database import Database
    from orb.core.system import System

    db = Database(MockConnection(), 'test-db')

    system = System()
    system.register_database(db)

    assert system.databases().keys() == ['test-db']
    assert system.database() is None
    assert system.database('test-db') == db

    with pytest.raises(errors.DatabaseNotFound):
        assert system.database('test-db2')

    # check for duplicate entries
    db2 = Database(MockConnection(), 'test-db')

    assert system.register(db) is None
    with pytest.raises(errors.DuplicateEntryFound):
        assert system.register(db2)


def test_create_and_register_schema():
    import orb
    from orb.core.system import System

    class User(orb.Table):
        __register__ = False
        id = orb.IdColumn()
        username = orb.StringColumn()

    system = System()
    system.register_schema(User.schema())

    assert system.schemas().keys() == ['User']
    assert system.schema('User') == User.schema()
    assert system.model('User') == User
    assert system.models().keys() == ['User']
    assert system.models().values() == [User]

    with pytest.raises(orb.errors.ModelNotFound):
        assert system.model('Group')
    with pytest.raises(orb.errors.ModelNotFound):
        assert system.schema('Group')

    # check for duplicate entries
    user2 = orb.Schema(name='User')
    assert system.register(User) is None
    with pytest.raises(orb.errors.DuplicateEntryFound):
        assert system.register(user2)


def test_adding_models_to_scope():
    import orb
    from orb.core.system import System

    class User(orb.Table):
        __register__ = False
        id = orb.IdColumn()
        username = orb.StringColumn()

    system = System()
    system.register(User)

    scope = {}
    system.add_models_to_scope(scope)
    assert scope['User'] == User


def test_auto_generate_model():
    import orb
    from orb.core.system import System

    # define the schema
    schema = orb.Schema(name='User')
    schema.register(orb.IdColumn(name='id'))
    schema.register(orb.StringColumn(name='username', default='testing'))

    # register the schema
    system = System()
    system.register(schema)

    assert system.schema('User') == schema

    with pytest.raises(orb.errors.ModelNotFound):
        assert system.model('User')

    model = system.model('User', auto_generate=True)
    assert issubclass(model, orb.Model)
    assert model.__name__ == schema.name()
    assert set(model.schema().columns().keys()) == {'id', 'username'}

    record = model()
    assert isinstance(record, model)
    assert record.schema() == schema
    assert record.get('username') == 'testing'


def test_model_filtering():
    import orb
    from orb.core.system import System

    class User(orb.Table):
        __register__ = False
        __group__ = 'auth'
        __database__ = 'testing-db'

        id = orb.IdColumn()
        username = orb.StringColumn()

    system = System()
    system.register(User)

    # no filtering
    assert system.models().keys() == ['User']

    # filter by group
    assert system.models(group='auth').keys() == ['User']
    assert system.models(group='custom').keys() == []

    # filter by database
    assert system.models(database='testing-db').keys() == ['User']
    assert system.models(database='test-db').keys() == []


def test_model_inheritance_filtering():
    import orb
    from orb.core.system import System

    system = System()

    class Fruit(orb.Table):
        __system__ = system
        __group__ = 'fruits'
        __database__ = 'fruits'

        id = orb.IdColumn()
        name = orb.StringColumn()

    class Apple(Fruit):
        __system__ = system

        granny_smith = orb.BooleanColumn()

    class Grape(Fruit):
        __system__ = system

        seedless = orb.BooleanColumn()

    class User(orb.Table):
        __system__ = system

        id = orb.IdColumn()

    # filter by base class
    assert set(system.models().keys()) == {'Fruit', 'Apple', 'Grape', 'User'}
    assert set(system.models(group='fruits').keys()) == {'Fruit', 'Apple', 'Grape'}
    assert set(system.models(base=Fruit).keys()) == {'Apple', 'Grape'}
    assert set(system.models(base=User).keys()) == set()

def test_invalid_registration():
    import orb
    from orb.core.system import System

    system = System()
    with pytest.raises(orb.errors.OrbError):
        system.register(10)


def test_schema_registration_and_filtering():
    import orb
    from orb.core.system import System

    system = System()

    class User(orb.Table):
        __system__ = system
        __group__ = 'auth'
        __database__ = 'users'

        id = orb.IdColumn()

    assert system.schemas().keys() == ['User']
    assert system.schemas(group='auth').keys() == ['User']
    assert system.schemas(database='users').keys() == ['User']


def test_object_unregistration():
    import orb
    from orb.testing import MockConnection
    from orb.core.system import System

    db = orb.Database(MockConnection(), 'test-db')
    system = System()

    class User(orb.Table):
        __system__ = system

        id = orb.IdColumn()

    class Group(orb.Table):
        __system__ = system

        id = orb.IdColumn()

    system.register(db)

    assert set(system.models().values()) == {User, Group}
    assert set(system.schemas().values()) == {User.schema(), Group.schema()}
    assert system.databases().values() == [db]

    system.unregister(User)
    system.unregister(Group.schema())
    system.unregister(db)

    assert system.models().values() == []
    assert system.schemas().values() == []
    assert system.databases().values() == []

    with pytest.raises(orb.errors.OrbError):
        assert system.unregister(10)

    system.unregister(User)
    system.unregister(Group.schema())
    system.unregister(db)


def test_deprecated_methods():
    import orb
    from orb.core.system import System

    system = System()

    class User(orb.Table):
        __system__ = system

        id = orb.IdColumn()

    scope = {}
    system.init(scope)
    assert scope['User'] == User
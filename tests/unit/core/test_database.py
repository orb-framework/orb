import pytest


@pytest.fixture()
def sync_setup():
    def wrapper(responses):
        import orb
        from orb.core.system import System
        from orb.core.database import Database
        from orb.testing import MockConnection

        conn = MockConnection(responses=responses)
        system = System()

        class User(orb.Table):
            __system__ = system
            __namespace__ = 'auth'

            id = orb.IdColumn()
            username = orb.StringColumn()

        class UserView(orb.View):
            __system__ = system
            __namespace__ = 'custom_views'
            __id__ = 'user'

            user = orb.ReferenceColumn('User')
            username = orb.StringColumn(shortcut='user.username')

        class Ignored(orb.Table):
            __system__ = system
            __flags__ = {'Abstract'}

            id = orb.IdColumn()

        return Database(connection=conn, system=system)
    return wrapper


def test_basic_database():
    from orb.errors import BackendNotFound
    from orb.core.database import Database

    db = Database()

    assert db.code() == ''
    assert db.name() == ''
    assert db.username() == ''
    assert db.password() == ''
    assert db.host() is None
    assert db.port() is None
    assert db.timeout() == 20000
    assert db.credentials() == ('', '')
    assert db.system() is None
    assert db.write_host() is None

    with pytest.raises(BackendNotFound):
        assert not db.connection()

    with pytest.raises(BackendNotFound):
        assert not db.set_connection('testing')

def test_basic_database_with_keys():
    from orb.core.database import Database
    from orb.testing import MockConnection

    conn = MockConnection()
    db = Database(
        conn,
        code='testing',
        username='jdoe',
        password='my.secret',
        host='127.0.0.1',
        port=5432,
        timeout=10
    )

    assert db.code() == 'testing'
    assert db.name() == 'testing'
    assert db.username() == 'jdoe'
    assert db.password() == 'my.secret'
    assert db.host() == '127.0.0.1'
    assert db.port() == 5432
    assert db.timeout() == 10
    assert db.credentials() == ('jdoe', 'my.secret')
    assert db.write_host() == '127.0.0.1'
    assert db.connection() == conn

    # test overrides
    db.set_username('jane.doe')
    db.set_password('her.password')
    db.set_host('125.0.0.1')
    db.set_port(1234)
    db.set_timeout(20)
    db.set_write_host('128.0.0.1')
    db.set_credentials(('abcd', 'efg'))
    db.set_name('testing-db')

    assert db.name() == 'testing-db'
    assert db.username() == 'jane.doe'
    assert db.password() == 'her.password'
    assert db.host() == '125.0.0.1'
    assert db.port() == 1234
    assert db.timeout() == 20
    assert db.write_host() == '128.0.0.1'
    assert db.credentials() == ('abcd', 'efg')

    # test resets
    db.set_write_host(None)
    db.set_credentials(None)
    db.set_name(None)

    assert db.name() == 'testing'
    assert db.write_host() == '125.0.0.1'
    assert db.credentials() == ('jane.doe', 'her.password')


def test_database_custom_system():
    from orb.core.database import Database
    from orb.core.system import System

    system = System()
    db = Database(code='orb-testing', system=system)

    assert db.code() == 'orb-testing'
    assert system.database('orb-testing') is db
    assert db.system() is system

    assert system.database() is None
    db.activate()
    assert system.database() is db


def test_database_default_system():
    import orb
    from orb.core.database import Database

    db = Database(code='orb-testing')

    assert orb.system.database() is None
    assert len(orb.system.databases()) == 0

    db.activate()

    assert orb.system.database() is db
    assert len(orb.system.databases()) == 1

    orb.system.unregister(db)


def test_database_system_switching():
    from orb.core.database import Database
    from orb.core.system import System

    a = System()
    b = System()

    db = Database(code='testing', system=a)

    assert db in a.databases().values()
    assert db not in b.databases().values()

    db.set_system(b)

    assert db not in a.databases().values()
    assert db in b.databases().values()

    db.set_system(b)

    assert db.system() == b


def test_connection_basics():
    from orb.core.database import Database
    from orb.testing import MockConnection

    checks = {}

    class CustomConnection(MockConnection):
        __plugin_name__ = 'Testing'

        def create_namespace(self, namespace, context):
            checks['namespace'] = namespace

        def cleanup(self):
            checks['cleanup'] = True

        def interrupt(self, thread_id=None):
            checks['interrupt'] = thread_id

    db = Database(CustomConnection())

    db.create_namespace('testing')
    db.cleanup()
    db.interrupt(10)

    assert not db.is_connected()
    assert checks['namespace'] == 'testing'
    assert checks['cleanup'] is True
    assert checks['interrupt'] == 10
    assert db.connection_type() == 'Testing'


def test_basic_database_syncing(sync_setup):
    db = sync_setup({
        'schema_info': {}
    })
    assert db.sync() is True


def test_database_syncing_signals(sync_setup):
    db = sync_setup({
        'schema_info': {}
    })

    checks = {}
    def about_to_sync(sender, event=None):
        checks['about_to_sync'] = sender

    def synced(sender, event=None):
        checks['synced'] = sender

    conn = db.connection()
    conn.about_to_sync.connect(about_to_sync, sender=conn)
    conn.synced.connect(synced, sender=conn)

    assert db.sync() is True
    assert checks['about_to_sync'] == conn
    assert checks['synced'] == conn


def test_database_syncing_blocked_signal(sync_setup):
    db = sync_setup({
        'schema_info': {}
    })

    checks = {}

    def about_to_sync(sender, event=None):
        checks['about_to_sync'] = sender
        event.preventDefault = True

    def synced(sender, event=None):
        checks['synced'] = sender

    conn = db.connection()
    conn.about_to_sync.connect(about_to_sync, sender=conn)
    conn.synced.connect(synced, sender=conn)

    assert db.sync() is False
    assert checks['about_to_sync'] == conn
    assert 'synced' not in checks


def test_database_syncing_with_updates(sync_setup):
    db = sync_setup({
        'schema_info': {
            'users': {
                'fields': ['id'],
                'indexes': []
            }
        }
    })

    assert db.sync() is True
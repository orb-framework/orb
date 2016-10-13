import pytest


@pytest.fixture()
def pg_db(request):
    import getpass
    import orb

    db = orb.Database('Postgres')
    db.set_name('orb_testing')
    db.set_host('localhost')
    db.set_username(getpass.getuser())
    db.set_password('')
    db.activate()

    def fin():
        db.disconnect()
        orb.system.unregister(db)

    request.addfinalizer(fin)

    return db

@pytest.fixture()
def pg_sql(pg_db):
    import orb
    return orb.Connection.get_plugin('Postgres')

@pytest.fixture(scope='session')
def pg_all_column_record(orb, TestAllColumns):
    record = TestAllColumns(password='T3st1ng!')
    return record

@pytest.fixture()
def pg_last_column_record(orb, TestAllColumns):
    record = TestAllColumns.select().last()
    return record

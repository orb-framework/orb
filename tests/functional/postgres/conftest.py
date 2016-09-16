import pytest


@pytest.fixture()
def pg_db(request):
    import getpass
    import orb

    db = orb.Database('Postgres')
    db.setName('orb_testing')
    db.setHost('localhost')
    db.setUsername(getpass.getuser())
    db.setPassword('')
    db.activate()

    def fin():
        db.disconnect()

    request.addfinalizer(fin)

    return db

@pytest.fixture()
def pg_sql(pg_db):
    import orb
    return orb.Connection.byName('Postgres')

@pytest.fixture(scope='session')
def pg_all_column_record(orb, TestAllColumns):
    record = TestAllColumns(password='T3st1ng!')
    return record

@pytest.fixture()
def pg_last_column_record(orb, TestAllColumns):
    record = TestAllColumns.select().last()
    return record

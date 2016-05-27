import pytest


@pytest.fixture()
def my_db():
    import getpass
    import orb

    db = orb.Database('MySQL')
    db.setName('orb_testing')
    db.setHost('localhost')
    db.setUsername('root')
    db.setPassword('')
    db.activate()

    def fin():
        db.disconnect()

    return db

@pytest.fixture()
def my_sql(my_db):
    import orb
    return orb.Connection.byName('MySQL')

@pytest.fixture(scope='session')
def my_all_column_record(orb, TestAllColumns):
    record = TestAllColumns(password='T3st1ng!')
    return record

@pytest.fixture()
def my_last_column_record(orb, TestAllColumns):
    record = TestAllColumns.select().last()
    return record

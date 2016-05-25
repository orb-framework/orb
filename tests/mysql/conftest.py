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
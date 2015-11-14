import pytest


@pytest.fixture(scope='session')
def pg_db():
    import getpass
    import orb

    db = orb.Database('Postgres')
    db.setName('orb_testing')
    db.setHost('localhost')
    db.setUsername(getpass.getuser())
    db.setPassword('')
    db.activate()

    return db

@pytest.fixture(scope='session')
def pg_sql():
    import orb
    return orb.Connection.byName('Postgres')
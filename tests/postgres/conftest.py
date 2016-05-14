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
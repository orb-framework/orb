import pytest

@pytest.fixture()
def lite_db():
    import orb

    db = orb.Database('SQLite')
    db.setName('orb_testing')
    db.activate()

    def fin():
        db.disconnect()

    return db

@pytest.fixture()
def lite_sql(lite_db):
    import orb
    return orb.Connection.byName('SQLite')

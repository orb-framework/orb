import pytest


@pytest.fixture()
def mock_db():
    import orb
    import orb.testing

    def _mock_db(*args, **kw):
        conn = orb.testing.MockConnection(*args, **kw)
        return orb.Database(conn)

    return _mock_db

@pytest.fixture()
def MockUser():
    import orb

    class MockUser(orb.Model):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn(flags={'Required'})
        password = orb.PasswordColumn(flags={'Required'})
        first_name = orb.StringColumn()
        last_name = orb.StringColumn()
        group = orb.ReferenceColumn('MockGroup', flags={'AutoExpand'})

    return MockUser

@pytest.fixture()
def MockGroup():
    import orb

    class MockGroup(orb.Model):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn(flags={'Required'})

    return MockGroup
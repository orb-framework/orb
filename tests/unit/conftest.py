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

    orb.system.register(MockUser.schema())
    yield MockUser
    orb.system.unregister(MockUser.schema())


@pytest.fixture()
def MockGroup(request):
    import orb

    class MockGroup(orb.Model):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn(flags={'Required'})

    orb.system.register(MockGroup.schema())
    yield MockGroup
    orb.system.unregister(MockGroup.schema())


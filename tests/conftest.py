import pytest

def pytest_runtest_makereport(item, call):
    if "incremental" in item.keywords:
        if call.excinfo is not None:
            parent = item.parent
            parent._previousfailed = item


def pytest_runtest_setup(item):
    previousfailed = getattr(item.parent, "_previousfailed", None)
    if previousfailed is not None:
        pytest.xfail("previous test failed (%s)" %previousfailed.name)

# --------------

@pytest.fixture(scope='session')
def empty_user_table():
    import orb

    class User(orb.Table):
        pass

    return User

@pytest.fixture(scope='session')
def user_table():
    import orb

    class User(orb.Table):
        id = orb.SerialColumn()
        username = orb.StringColumn(flags=orb.Column.Flags.Unique, index=orb.Column.Index(name='byUsername'))
        password = orb.PasswordColumn()

    return User

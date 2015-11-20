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
def orb():
    import orb
    from projex import security

    key = security.generateKey('T3st!ng')
    orb.system.security().setKey(key)

    return orb

@pytest.fixture(scope='session')
def EmptyUser(orb):
    import orb

    class EmptyUser(orb.Table):
        pass

    return EmptyUser

@pytest.fixture(scope='session')
def testing_schema(orb):

    class Group(orb.Table):
        id = orb.IdColumn()
        name = orb.StringColumn(flags={'Unique'}, index=orb.Column.Index(name='byName'))
        owner = orb.ReferenceColumn(reference='User')

        users = orb.Pipe('users', through='GroupUser', source='group', target='user')

    class User(orb.Table):
        id = orb.IdColumn()
        username = orb.StringColumn(flags={'Unique'}, index=orb.Column.Index(name='byUsername'))
        password = orb.PasswordColumn()
        type = orb.StringColumn(flags={'Polymorphic'}, default='User')

        groups = orb.Pipe('groups', through='GroupUser', source='user', target='group')

    class GroupUser(orb.Table):
        id = orb.IdColumn()
        user = orb.ReferenceColumn(reference='User', reverse=orb.ReferenceColumn.Reversed(name='userGroups'))
        group = orb.ReferenceColumn(reference='Group', reverse=orb.ReferenceColumn.Reversed(name='groupUsers'))

        byUserAndGroup = orb.Index(('user', 'group'), unique=True)
        byUser = orb.Index(('user',))

    class Document(orb.Table):
        id = orb.IdColumn()
        title = orb.StringColumn(flags={'Translatable'})
        description = orb.TextColumn(flags={'Translatable'})

    class Role(orb.Table):
        id = orb.IdColumn()
        name = orb.StringColumn()

    class Employee(User):
        role = orb.ReferenceColumn(reference='Role')

    return locals()

@pytest.fixture(scope='session')
def User(testing_schema):
    return testing_schema['User']

@pytest.fixture(scope='session')
def GroupUser(testing_schema):
    return testing_schema['GroupUser']

@pytest.fixture(scope='session')
def Group(testing_schema):
    return testing_schema['Group']

@pytest.fixture(scope='session')
def Document(testing_schema):
    return testing_schema['Document']

@pytest.fixture(scope='session')
def Role(testing_schema):
    return testing_schema['Role']

@pytest.fixture(scope='session')
def Employee(testing_schema):
    return testing_schema['Employee']
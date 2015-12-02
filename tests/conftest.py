import pytest
from projex.enum import enum


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
    class EmptyUser(orb.Table):
        pass

    return EmptyUser

@pytest.fixture(scope='session')
def TestAllColumns(orb):
    class TestReference(orb.Table):
        id = orb.IdColumn()

    class TestAllColumns(orb.Table):
        Test = enum('Ok')

        # boolean
        bool = orb.BooleanColumn()

        # data
        binary = orb.BinaryColumn()
        json = orb.JSONColumn()
        query = orb.QueryColumn()
        yaml = orb.YAMLColumn()

        # datetime
        date = orb.DateColumn()
        datetime = orb.DatetimeColumn()
        datetime_tz = orb.DatetimeWithTimezoneColumn()
        interval = orb.IntervalColumn()
        time = orb.TimeColumn()
        timestamp = orb.TimestampColumn()
        utc_datetime = orb.UTC_DatetimeColumn()
        utc_timestamp = orb.UTC_TimestampColumn()

        # numberic
        id = orb.IdColumn()
        decimal = orb.DecimalColumn()
        float = orb.FloatColumn()
        integer = orb.IntegerColumn()
        long = orb.LongColumn()
        enum = orb.EnumColumn()

        # reference
        reference = orb.ReferenceColumn(reference='TestReference')

        # string
        string = orb.StringColumn()
        text = orb.TextColumn()
        color = orb.ColorColumn()
        directory = orb.DirectoryColumn()
        email = orb.EmailColumn()
        filepath = orb.FilepathColumn()
        html = orb.HtmlColumn()
        password = orb.PasswordColumn()
        token = orb.TokenColumn()
        url = orb.UrlColumn()
        xml = orb.XmlColumn()

    return TestAllColumns

@pytest.fixture(scope='session')
def all_column_record(orb, TestAllColumns):
    record = TestAllColumns(password='T3st1ng!')
    return record

@pytest.fixture(scope='session')
def last_column_record(orb, TestAllColumns):
    record = TestAllColumns.select().last()
    return record

@pytest.fixture(scope='session')
def testing_schema(orb):

    class Group(orb.Table):
        id = orb.IdColumn()
        name = orb.StringColumn(flags={'Unique'}, index=orb.Column.Index(name='byName'))
        owner = orb.ReferenceColumn(reference='User')

        users = orb.Pipe('users', through='GroupUser', from_='group', to='user')

    class User(orb.Table):
        id = orb.IdColumn()
        username = orb.StringColumn(flags={'Unique'}, index=orb.Column.Index(name='byUsername'))
        password = orb.PasswordColumn()
        token = orb.TokenColumn()

        groups = orb.Pipe('groups', through='GroupUser', from_='user', to='group')

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
def Employee(testing_schema):
    return testing_schema['Employee']

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
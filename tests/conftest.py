import pytest
import logging
from projex.enum import enum

logging.basicConfig()


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

        # numeric
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
        password = orb.PasswordColumn(default='T3st1ng!')
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
        __resource__ = True

        id = orb.IdColumn()
        name = orb.StringColumn(flags={'Unique'})
        owner = orb.ReferenceColumn(reference='User')

        users = orb.Pipe(through_path='GroupUser.group.user')
        groupUsers = orb.ReverseLookup(from_column='GroupUser.group')

        byName = orb.Index(columns=['name'], flags={'Unique'})

    class User(orb.Table):
        __resouce__ = True

        id = orb.IdColumn()
        username = orb.StringColumn(flags={'Unique'})
        password = orb.PasswordColumn()
        token = orb.TokenColumn()

        groups = orb.Pipe(through='GroupUser', from_='user', to='group')
        userGroups = orb.ReverseLookup(from_column='GroupUser.user')

        @orb.virtual(orb.BooleanColumn)
        def hasGroups(self):
            return len(self.groups()) != 0

        @orb.virtual(orb.Collector, model='Group')
        def myGroups(self, **context):
            group_ids = GroupUser.select(where=orb.Query('user') == self).values('group')
            context['where'] = orb.Query('id').in_(group_ids) & context.get('where')
            return Group.select(**context)

        byUsername = orb.Index(columns=['username'], flags={'Unique'})

    class GroupUser(orb.Table):
        id = orb.IdColumn()
        user = orb.ReferenceColumn(reference='User')
        group = orb.ReferenceColumn(reference='Group')

        byUserAndGroup = orb.Index(('user', 'group'), flags={'Unique'})
        byUser = orb.Index(('user',))

    class Document(orb.Table):
        id = orb.IdColumn()
        title = orb.StringColumn(flags={'I18n'})
        description = orb.TextColumn(flags={'I18n'})

    class Role(orb.Table):
        id = orb.IdColumn()
        name = orb.StringColumn()

    class Employee(User):
        role = orb.ReferenceColumn(reference='Role')

    class Comment(orb.Table):
        id = orb.IdColumn(type='hash')
        text = orb.TextColumn()
        attachments = orb.ReverseLookup(from_column='Attachment.comment')

    class Attachment(orb.Table):
        id = orb.IdColumn(type='hash')
        filename = orb.StringColumn()
        comment = orb.ReferenceColumn(reference='Comment')

    return locals()

@pytest.fixture(scope='session')
def Comment(testing_schema):
    return testing_schema['Comment']

@pytest.fixture(scope='session')
def Attachment(testing_schema):
    return testing_schema['Attachment']

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
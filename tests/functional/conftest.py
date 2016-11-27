import pytest
import logging
from projex.enum import enum

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

@pytest.fixture(scope='session')
def orb():
    import orb
    from projex import security

    key = security.generateKey('T3st!ng')
    orb.system.settings.security_key = key

    return orb

@pytest.fixture(scope='session')
def mock_db():
    import orb
    import orb.testing

    # creating the database
    db = orb.Database(orb.testing.MockConnection(), 'testing')
    db.activate()

    return db

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
        decimal = orb.DecimalColumn(scale=2)
        float = orb.FloatColumn()
        integer = orb.IntegerColumn()
        long = orb.LongColumn()
        enum = orb.EnumColumn()

        # reference
        reference = orb.ReferenceColumn('TestReference')

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
def namespace_models(orb):
    class TestDefault(orb.Table):
        id = orb.IdColumn()
        name = orb.StringColumn()

        @classmethod
        def on_sync(cls, event):
            print 'syncing defaults'
            cls.ensure_exists({'name': 'test'})
            super(TestDefault, cls).on_sync(event)

    class TestExplicit(orb.Table):
        __namespace__ = 'test_explicit'

        id = orb.IdColumn()
        name = orb.StringColumn()

        @classmethod
        def on_sync(cls, event):
            print 'syncing explicit'
            cls.ensure_exists({'name': 'test'})
            super(TestExplicit, cls).on_sync(event)

    return {'TestDefault': TestDefault, 'TestExplicit': TestExplicit}

@pytest.fixture(scope='session')
def testing_schema(orb):
    class Group(orb.Table):
        __resource__ = True

        id = orb.IdColumn()
        name = orb.StringColumn(flags={'Unique'})
        owner = orb.ReferenceColumn('User')

        users = orb.Pipe('GroupUser.group.user')
        groupUsers = orb.ReverseLookup('GroupUser.group')

        byName = orb.Index(columns=['name'], flags={'Unique'})

    class UserType(orb.Table):
        id = orb.IdColumn()
        code = orb.StringColumn(flags={'Required', 'Unique', 'Keyable'})

        @classmethod
        def on_sync(cls, event):
            for code in ('basic', 'superuser'):
                UserType.ensure_exists({'code': code})

            super(UserType, cls).on_sync(event)

    class User(orb.Table):
        __resouce__ = True

        id = orb.IdColumn()
        username = orb.StringColumn(flags={'Unique'})
        password = orb.PasswordColumn()
        token = orb.TokenColumn()
        user_type = orb.ReferenceColumn('UserType', default='basic')

        groups = orb.Pipe('GroupUser.user.group')
        userGroups = orb.ReverseLookup('GroupUser.user')

        @orb.virtual(orb.BooleanColumn)
        def hasGroups(self, **context):
            return len(self.get('groups')) != 0

        @orb.virtual(orb.Collector, model='Group')
        def myGroups(self, **context):
            group_ids = GroupUser.select(where=orb.Query('user') == self).values('group')
            context['where'] = orb.Query('id').in_(group_ids) & context.get('where')
            return Group.select(**context)

        byUsername = orb.Index(columns=['username'], flags={'Unique'})

    class GroupUser(orb.Table):
        id = orb.IdColumn()
        user = orb.ReferenceColumn('User')
        group = orb.ReferenceColumn('Group')

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
        role = orb.ReferenceColumn('Role', flags={'AutoExpand'})

    class Comment(orb.Table):
        id = orb.IdColumn(type='hash')
        text = orb.TextColumn()
        attachments = orb.ReverseLookup('Attachment.comment', flags={'AutoExpand'})

    class Attachment(orb.Table):
        id = orb.IdColumn(type='hash')
        filename = orb.StringColumn()
        comment = orb.ReferenceColumn('Comment', flags={'Required'})

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
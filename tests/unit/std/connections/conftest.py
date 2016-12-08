import pytest


@pytest.fixture()
def User():
    import orb

    class User(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        username = orb.StringColumn(flags={'Required', 'Unique'})
        password = orb.StringColumn(flags={'Required', 'Encrypted'})

        by_username = orb.Index(['username'], flags={'Unique'})

    return User


@pytest.fixture()
def Employee(User):
    import orb

    class Employee(User):
        first_name = orb.StringColumn()
        last_name = orb.StringColumn()

        by_first_and_last_name = orb.Index(('first_name', 'last_name'))

    return Employee

@pytest.fixture()
def Comment():
    import orb

    class Comment(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        text = orb.TextColumn()

    return Comment


@pytest.fixture()
def Page():
    import orb

    class Page(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        title = orb.StringColumn(flags={'I18n'})
        body = orb.TextColumn(flags={'I18n'})

    return Page


@pytest.fixture()
def Incrementer():
    import orb

    class Incrementer(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        count = orb.IntegerColumn(flags={'AutoIncrement'})

    return Incrementer


@pytest.fixture()
def StandardColumn():
    import orb

    class StandardColumn(orb.Table):
        __system__ = orb.System()
        __namespace__ = 'testing'

        id = orb.IdColumn()

        # boolean
        bool_test = orb.BooleanColumn()

        # data
        binary_test = orb.BinaryColumn()
        json_test = orb.JSONColumn()
        query_test = orb.QueryColumn()
        yaml_test = orb.YAMLColumn()

        # dtime
        date_test = orb.DateColumn()
        datetime_test = orb.DatetimeColumn()
        datetime_tz_test = orb.DatetimeWithTimezoneColumn()
        interval_test = orb.IntervalColumn()
        time_test = orb.TimeColumn()
        timestamp_test = orb.TimestampColumn()

        # numeric
        decimal_test = orb.DecimalColumn()
        float_test = orb.FloatColumn()
        integer_test = orb.IntegerColumn()
        long_test = orb.LongColumn()
        enum_column = orb.EnumColumn()

        # reference column
        parent_test = orb.ReferenceColumn('StandardColumn')
        parent_string_test = orb.ReferenceColumn('StandardColumn', 'string_test')

        # string column
        string_test = orb.StringColumn()
        text_test = orb.TextColumn()

    return StandardColumn
import pytest


def test_basic_column_creation():
    from orb.core.column import Column

    column = Column()

    assert column.name() == ''
    assert column.shortcut() == ''
    assert column.display() == ''
    assert column.flags() == 0
    assert column.default() is None
    assert column.schema() is None
    assert column.order() == 99999

    assert column.alias() == ''
    assert column.field() == ''

    assert column.read_permit() is None
    assert column.write_permit() is None

    assert column.gettermethod() is None
    assert column.settermethod() is None
    assert column.filtermethod() is None


def test_basic_column_creation_with_settings():
    from orb.core.column import Column

    a = Column(name='userName')
    b = Column(name='userName', field='username')
    c = Column(name='userName', field='fkey_username', alias='username_id')

    assert a.name() == 'userName'
    assert a.field() == 'user_name'
    assert a.alias() == 'user_name'

    assert b.name() == 'userName'
    assert b.field() == 'username'
    assert b.alias() == 'username'

    assert c.name() == 'userName'
    assert c.field() == 'fkey_username'
    assert c.alias() == 'username_id'


def test_set_basic_column_properties():
    from orb.core.column import Column

    column = Column()

    column.set_name('userName')
    assert column.name() == 'userName'
    assert column.field() == 'user_name'
    assert column.alias() == 'user_name'
    assert column.display() == 'User Name'

    column.set_name('firstName')
    assert column.name() == 'firstName'
    assert column.field() == 'first_name'
    assert column.alias() == 'first_name'
    assert column.display() == 'First Name'

    column.set_display('Name')
    assert column.display() == 'Name'

    column.set_field('fname')
    assert column.field() == 'fname'
    assert column.alias() == 'fname'

    column.set_alias('field_name')
    assert column.field() == 'fname'
    assert column.alias() == 'field_name'

    assert column.default() is None
    column.set_default(10)
    assert column.default() == 10
    column.set_default(lambda x: x.name())
    assert column.default() == 'firstName'

    assert column.read_permit() is None
    assert column.write_permit() is None

    column.set_read_permit('test_read')
    column.set_write_permit('test_write')

    assert column.read_permit() == 'test_read'
    assert column.write_permit() == 'test_write'

    assert column.order() == 99999
    column.set_order(0)
    assert column.order() == 0


def test_column_modify_shortcut_and_flags():
    from orb.core.column import Column

    column = Column()
    assert column.test_flag(Column.Flags.Virtual) is False
    column.set_shortcut('some.test')
    assert column.test_flag(Column.Flags.Virtual) is True
    column.set_flag(Column.Flags.Virtual, False)
    assert column.test_flag(Column.Flags.Virtual) is False
    column.set_flag(Column.Flags.Virtual)
    assert column.test_flag(Column.Flags.Virtual) is True


def test_column_comparison_and_equality():
    from orb.core.column import Column

    a = Column()
    b = Column()
    c = Column(order=1)

    assert a == a
    assert a != b
    assert a.__cmp__(a) == 0
    assert cmp(a, a) == 0
    assert cmp(a, 1) == -1
    assert cmp(a, b) == 0
    assert cmp(a, c) == 1
    assert cmp(c, b) == -1
    assert cmp(c, c) == 0


def test_column_serialize_to_dict(assert_dict_equals):
    from orb.core.column import Column

    a = Column(name='userName')
    adict = dict(a)

    expected = {
        'name': 'userName',
        'shortcut': '',
        'display': '',
        'flags': 0,
        'default': None,
        'order': 99999,
        'alias': '',
        'field': '',
        'read_permit': None,
        'write_permit': None,
        'getter': None,
        'setter': None,
        'filter': None
    }

    assert set(adict.keys()).symmetric_difference(expected.keys()) == set()
    assert_dict_equals(adict, expected)


def test_column_serialize_to_json(assert_dict_equals):
    from orb.core.column import Column
    import orb

    class MockUser(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    class StringColumn(Column):
        pass

    user = MockUser({'id': 1})
    user.mark_loaded()

    assert user.get('id') == 1

    column = StringColumn(
        name='username',
        field='fkey_username',
        alias='username',
        default=user,
        flags={'Required'}
    )

    response = {
        'type': 'String',
        'name': 'username',
        'field': 'username',
        'display': 'Username',
        'flags': {
            'Required': True
        },
        'default': 1
    }

    jdata = column.__json__()
    assert_dict_equals(jdata, response)



def test_column_with_shortcut_is_virtual():
    from orb.core.column import Column

    a = Column(name='testing', shortcut='path.to.something')
    assert a.shortcut() == 'path.to.something'
    assert a.test_flag(a.Flags.Virtual)


def test_column_with_schema_on_creation():
    from orb.core.column import Column
    from orb.core.schema import Schema

    schema = Schema()
    column = Column(name='testing', schema=schema)

    assert column.schema() == schema
    assert schema.column('testing') == column


def test_copying_column_preserves_class_and_state(assert_dict_equals):
    from orb.core.column import Column

    class StringColumn(Column):
        pass

    def gettermethod():
        return 10

    a = StringColumn(
        name='userName',
        display='Full Name',
        getter=gettermethod
    )
    b = a.copy()

    assert a.name() == 'userName'
    assert a.field() == 'user_name'
    assert a.display() == 'Full Name'
    assert a.gettermethod() == gettermethod
    assert type(b) is StringColumn

    assert_dict_equals(dict(a), dict(b))


def test_support_defaults_as_callable_methods():
    from orb.core.column import Column

    def get_default_value(column):
        assert column.name() == 'testing'
        return 'DEFAULT FOUND'

    a = Column(
        name='testing',
        default=get_default_value
    )
    assert a.default() == 'DEFAULT FOUND'


def test_decorator_methods():
    from orb.core.column import Column

    checks = {}

    def getter(*args):
        checks['get'] = True
        return 10

    def setter(*args):
        checks['set'] = True

    def filter(*args):
        checks['filter'] = True

    a = Column(
        getter=getter,
        setter=setter,
        filter=filter
    )

    b = Column()
    b.getter(getter)
    b.setter(setter)
    b.filter(filter)

    c = Column()
    c.getter()(getter)
    c.setter()(setter)
    c.filter()(filter)

    assert a.gettermethod() == getter
    assert a.settermethod() == setter
    assert a.filtermethod() == filter

    assert b.gettermethod() == getter
    assert b.settermethod() == setter
    assert b.filtermethod() == filter

    assert c.gettermethod() == getter
    assert c.settermethod() == setter
    assert c.filtermethod() == filter


def test_column_empty_vs_null_values():
    from orb.core.column import Column

    a = Column()

    assert a.is_null(None)
    assert not a.is_null('')
    assert not a.is_null(False)

    assert a.is_empty(None)
    assert a.is_empty('')
    assert a.is_empty(False)


def test_value_conversions():
    from orb.core.column import Column

    a = Column()
    assert a.value_from_string('10') == '10'
    assert a.value_to_string('10') == '10'


def test_column_random_value():
    from orb.core.column import Column

    a = Column(default=10)
    assert a.random_value() == 10


def test_basic_column_requirement_validation():
    import orb
    from orb.core.column import Column

    a = Column(flags={'Required'})
    assert a.validate(1)
    assert a.validate('')

    with pytest.raises(orb.errors.ColumnValidationError):
        assert not a.validate(None)


def test_internationalization_column_store():
    import orb
    from orb.core.column import Column

    a = Column(flags={'I18n'})
    assert a.store(10) == {'en_US': 10}
    assert a.store({'en_US': 20}) == {'en_US': 20}


    context = orb.Context(locale='fr_FR')
    with context:
        assert a.store(10) == {'fr_FR': 10}
        assert a.store({'en_US': 20}) == {'en_US': 20}

    assert a.store(20, context=context) == {'fr_FR': 20}


def test_internationalization_column_restore():
    import orb
    from orb.core.column import Column

    a = Column(flags={'I18n'})

    fr_context = orb.Context(locale='fr_FR')
    en_fr_context = orb.Context(locale='en_US,fr_FR')
    de_context = orb.Context(locale='de_DE')
    all_context = orb.Context(locale='all,en_US')

    locale_data = {
        'en_US': 'en_US testing',
        'fr_FR': 'fr_FR testing',
        'sp_SP': 'sp_SP testing'
    }

    assert a.restore(locale_data) == locale_data['en_US']
    assert a.restore(locale_data, context=de_context) == None
    assert a.restore(locale_data, context=fr_context) == locale_data['fr_FR']
    assert a.restore(locale_data, context=all_context) == locale_data
    assert a.restore(locale_data['en_US']) == locale_data['en_US']
    assert a.restore(locale_data['en_US'], context=all_context) == {'en_US': locale_data['en_US']}
    assert a.restore(locale_data, context=en_fr_context) == {'en_US': locale_data['en_US'],
                                                             'fr_FR': locale_data['fr_FR']}
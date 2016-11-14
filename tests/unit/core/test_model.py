import pytest


def test_basic_model_constructor():
    import orb

    class BasicModel(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    record = BasicModel()
    assert record.get('name') is None


def test_model_constructor_with_defaults():
    import orb

    class BasicModel(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn(default='testing')

    record = BasicModel()
    assert record.get('name') == 'testing'


def test_model_constructor_with_i18n_defaults():
    import orb

    class BasicModel(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn(flags={'I18n'}, default='testing')

    record = BasicModel()
    assert record.get('name') == 'testing'
    assert record.get('name', locale='fr_FR') is None
    assert record.get('name', locale='en_US') == 'testing'

    context = orb.Context(locale='fr_FR')
    assert context.locale == 'fr_FR'

    record = BasicModel(context=context)

    assert record.context().locale == 'fr_FR'
    assert record.get('name') == 'testing'
    assert record.get('name', locale='fr_FR') == 'testing'
    assert record.get('name', locale='en_US') is None


def test_model_constructor_with_loader():
    import orb

    class BasicModel(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    record = BasicModel({'id': 1, 'name': 'Testing'})
    record.mark_loaded()

    assert record.is_loaded()
    assert record.is_record()
    assert record.get('id') == 1
    assert record.get('name') == "Testing"


def test_model_constructor_with_multiple_column_id():
    import orb

    class BasicModel(orb.Table):
        __register__ = False


def test_model_polymorphism():
    import orb

    system = orb.System()

    class Food(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        class_type = orb.StringColumn(flags={'Polymorphic'})

    class Fruit(Food):
        pass

    class Apple(Fruit):
        pass

    class Vegetable(Food):
        pass

    a = Food()
    b = Fruit()
    c = Apple()
    d = Vegetable()

    assert a.get('class_type') == 'Food'
    assert b.get('class_type') == 'Fruit'
    assert c.get('class_type') == 'Apple'
    assert d.get('class_type') == 'Vegetable'

    assert len(orb.system.models()) == 0
    assert len(system.models()) == 4


def test_model_initialization_with_values():
    import orb

    class BasicModel(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()
        display_name = orb.StringColumn()

    record = BasicModel({'name': 'a', 'display_name': 'A'})

    assert not record.is_record()
    assert record.get('id') is None
    assert record.get('name') == 'a'
    assert record.get('display_name') == 'A'


def test_model_initialization_from_id(mock_db):
    import orb

    def get_record(*args):
        return [{
            'id': 1,
            'name': 'a',
            'display_name': 'A'
        }]

    class BasicModel(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()
        display_name = orb.StringColumn()

    db = mock_db(responses={'select': get_record})
    record = BasicModel(1, db=db)

    assert record.is_record()
    assert record.get('id') == 1
    assert record.get('name') == 'a'
    assert record.get('display_name') == 'A'


def test_model_initialization_from_tuple_id(mock_db):
    import orb

    def get_record(*args):
        return [{
            'id': ('table', 1),
            'name': 'a',
            'display_name': 'A'
        }]

    class BasicModel(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()
        display_name = orb.StringColumn()

    db = mock_db(responses={'select': get_record})
    record = BasicModel(1, db=db)

    assert record.is_record()
    assert record.get('id') == ('table', 1)
    assert record.get('name') == 'a'
    assert record.get('display_name') == 'A'


def test_model_initialization_record_not_found(mock_db):
    import orb

    class BasicModel(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()
        display_name = orb.StringColumn()

    db = mock_db()
    with pytest.raises(orb.errors.RecordNotFound):
        assert BasicModel(1, db=db) is None


def test_model_initialization_duplication():
    import orb

    class BasicModel(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = BasicModel({'id': 1, 'name': 'Testing'})
    a.mark_loaded()

    b = BasicModel(a)

    assert b.id() == a.id()
    assert b.get('name') == a.get('name')
    assert a.is_record()
    assert not b.is_record()
    assert a != b


def test_model_casting_from_inheritance(mock_db):
    import orb

    system = orb.System()

    class A(orb.Table):
        __system__ = system

        id = orb.IdColumn()

    class B(A):
        name = orb.StringColumn()

    class C(A):
        title = orb.StringColumn()

    db = mock_db(responses={
        'select': ({'id': 1, 'name': 'testing'},)
    })


    a = A({'id': 1})
    a.mark_loaded()

    b = B(a, db=db)
    assert b.id() == a.id()
    assert b.get('name') == 'testing'

    with pytest.raises(orb.errors.RecordNotFound):
        assert C(b, db=db) is None


def test_model_delay_loading():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A(1, delayed=True)
    assert a.id() == 1
    assert not a.is_record()
    assert not a.is_loaded()


def test_model_equality_by_instance():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    a = A()
    b = A()

    assert a == a
    assert a != b


def test_model_equality_by_type():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    class B(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    a = A()
    b = B()

    assert (a == b) is False
    assert (a != b) is True


def test_model_equality_by_id_and_attributes():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A({'id': 1})
    b = A({'id': 1})
    c = A({'id': 2})

    a.mark_loaded()
    b.mark_loaded()
    c.mark_loaded()

    assert (a == b) is True
    assert (a != b) is False
    assert (a == c) is False
    assert (a != c) is True

    b.set('name', 'bob')

    assert (a == b) is False
    assert (a != b) is True

    b.reset()
    a.set('name', 'bob')

    assert (a == b) is False
    assert (a != b) is True


def test_model_length_matches_dict_keys():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A()
    assert len(a) == 2
    assert len(a) == len(dict(a))


def test_model_get_item():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A({'id': 1, 'name': 'testing'})
    assert a['id'] == 1
    assert a['name'] == 'testing'

    with pytest.raises(KeyError):
        assert a['blah'] is not None


def test_model_set_item():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A({'id': 1, 'name': 'testing'})

    a['name'] = 'testing2'

    assert a['name'] == 'testing2'

    with pytest.raises(KeyError):
        a['blah'] = 'testing3'


def test_model_standard_json_serialization():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A({'id': 1, 'name': 'testing'})
    jdata = a.__json__()

    assert type(jdata) == dict
    assert jdata == {'id': 1, 'name': 'testing'}


def test_model_text_json_serialization():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A({'id': 1, 'name': 'testing'})
    a.set_context(a.context(format='text'))
    jdata = a.__json__()

    assert type(jdata) in (str, unicode)
    assert '"name": "testing"' in jdata
    assert '"id": 1' in jdata


def test_model_value_json_serialization():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A({'id': 1, 'name': 'testing'})
    a.set_context(a.context(returning='values'))
    jdata = a.__json__()

    assert type(jdata) == tuple
    assert jdata in ((1, 'testing'), ('testing', 1))


def test_model_single_value_json_serialization():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A({'id': 1, 'name': 'testing'})
    a.set_context(a.context(returning='values', columns='name'))
    jdata = a.__json__()

    assert jdata == 'testing'


def test_model_change_calculation():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A({'id': 1, 'name': 'testing'})

    import pprint
    pprint.pprint(a.changes())

    assert a.changes() == {a.schema().column('id'): (None, 1), a.schema().column('name'): (None, 'testing')}
    assert a.is_modified()

    a.mark_loaded()

    assert a.changes() == {}
    assert not a.is_modified()

    a.set('name', u'testing')

    assert a.changes() == {}
    assert not a.is_modified()

    a.set('name', u'testing2')

    assert a.changes() == {a.schema().column('name'): ('testing', u'testing2')}
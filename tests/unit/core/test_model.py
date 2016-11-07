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

    event = orb.LoadEvent({'id': 1, 'name': 'Testing'})
    record = BasicModel(loadEvent=event)
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

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


def test_model_quality_across_databases(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    db_a = mock_db()
    db_b = mock_db()

    a = A(db=db_a)
    b = A(db=db_b)

    assert (a == b) is False
    assert (a != b) is True


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
        value = orb.StringColumn(flags={'ReadOnly'})

    a = A({'id': 1, 'name': 'testing', 'value': 12})

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


def test_model_deletion(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    db = mock_db()

    a = A()
    assert a.delete() is False

    b = A({'id': 1}, db=db)
    b.mark_loaded()
    assert b.delete() is False

    def delete(*args, **kwargs):
        print(args)
        print(kwargs)
        return None, 1

    db = mock_db(responses={'delete': delete})
    c = A({'id': 1}, db=db)
    c.mark_loaded()
    assert c.delete() is True


def test_model_delete_event(mock_db):
    import orb

    checked = {}

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

        def on_delete(self, event):
            checked['deleted'] = True
            return super(A, self).on_delete(event)

    a = A({'id': 1}, db=mock_db(responses={'delete': (None, 1)}))
    a.mark_loaded()

    assert a.delete() is True
    assert checked['deleted'] is True


def test_model_delete_event_bound_to_class(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    class B(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    db = mock_db(responses={'delete': (None, 1)})

    a = A({'id': 1}, db=db)
    a.mark_loaded()

    b = B({'id': 2}, db=db)
    b.mark_loaded()

    checked = {}
    def block_deleted(sender, event):
        assert sender is A
        event.prevent_default = True
        checked['deleted'] = True

    orb.Model.deleted.connect(block_deleted, sender=A)

    try:
        assert a.delete() is False
        assert b.delete() is True
        assert checked['deleted'] is True
    finally:
        orb.Model.deleted.disconnect(block_deleted)


def test_model_delete_event_bound_to_instance(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    db = mock_db(responses={'delete': (None, 1)})

    a = A({'id': 1}, db=db)
    a.mark_loaded()

    b = A({'id': 1}, db=db)
    b.mark_loaded()

    checked = {}
    def block_deleted(sender, event):
        assert sender is a
        event.prevent_default = True
        checked['deleted'] = True

    orb.Model.deleted.connect(block_deleted, sender=a)

    try:
        assert a.delete() is False
        assert b.delete() is True
        assert checked['deleted'] is True
    finally:
        orb.Model.deleted.disconnect(block_deleted)


def test_model_get_shortcut():
    import orb

    system = orb.System()

    class A(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent = orb.ReferenceColumn('A')

    super_parent = A({'id': 1, 'name': 'super_parent', 'parent': None})
    parent = A({'id': 2, 'name': 'parent', 'parent': super_parent})
    child = A({'id': 3, 'name': 'child', 'parent': parent})

    super_parent.mark_loaded()
    parent.mark_loaded()
    child.mark_loaded()

    # validate base shortcuts
    assert child.get('parent.name') == 'parent'
    assert child.get('parent.parent') == super_parent
    assert child.get('parent.parent.name') == 'super_parent'

    # validate end chain does not fail
    assert child.get('parent.parent.parent.name') is None

    # validate shortcuts with expansion
    assert child.get('parent.name', expand='a,b') == 'parent'
    assert child.get('parent.name', expand=['a', 'b']) == 'parent'
    assert child.get('parent.name', expand={'a': {'b': {}}}) == 'parent'


def test_model_get_attribute():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn(default='testing')

    a = A()
    assert a.get('name') == 'testing'


def test_model_get_attribute_returning_based_on_field():
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        parent = orb.ReferenceColumn('A', alias='parent_id', field='fkey_a_parent_id')

    a = A({'id': 1})
    b = A({'id': 2, 'parent': a})

    assert b.get('parent_id') == 1
    assert b.get('fkey_a_parent_id') == 1
    assert b.get('parent') is a


def test_model_get_collection():
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

    a = A({'id': 1})

    assert isinstance(a.get('children'), orb.Collection)


def test_model_get_invalid_raises_column_error():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    a = A()
    with pytest.raises(orb.errors.ColumnNotFound):
        assert a.get('name') == ''


def test_model_get_attribute_shortcut():
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        parent = orb.ReferenceColumn('A')
        name = orb.StringColumn()
        parent_name = orb.StringColumn(shortcut='parent.name')

    class V(orb.View):
        __system__ = A.schema().system()
        __id__ = 'base'

        base = orb.ReferenceColumn('A')
        base_name = orb.StringColumn(shortcut='base.name')


    parent = A({'id': 1, 'name': 'parent'})
    child = A({'id': 2, 'name': 'child', 'parent': parent})

    assert child.get('parent.name') == 'parent'
    assert child.get('parent_name') == 'parent'

    v = V({'base': parent, 'base_name': 'testing'})
    v.mark_loaded()

    assert v.get('base_name') == 'testing'


def test_model_custom_getter_method():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn(default='testing')

        @name.getter()
        def get_name(self, **context):
            return '[name] ' + self.get('name', use_method=False, **context)

    a = A()
    assert a.get('name') == '[name] testing'
    assert a.get('name', use_method=False) == 'testing'


def test_model_triggers_read_for_unloaded(mock_db):
    import orb

    def get_record(*args, **kw):
        return [{
            'id': 1,
            'first_name': 'john',
            'last_name': 'doe'
        }]

    db = mock_db(responses={'select': get_record})

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        first_name = orb.StringColumn()
        last_name = orb.StringColumn()

        @orb.virtual(orb.StringColumn)
        def display_name(self, **context):
            return '{0} {1}'.format(self.get('first_name'), self.get('last_name'))

    a = A({'id': 1}, db=db)
    assert a.get('id') == 1
    assert not a.is_loaded()
    assert a.get('first_name') == 'john'
    assert a.get('display_name') == 'john doe'


def test_model_get_attribute_inflates_model(mock_db):
    import orb

    def get_record(*args, **kw):
        return [{
            'id': 1,
            'name': 'testing'
        }]

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent = orb.ReferenceColumn('A')

    db = mock_db(responses={'select': get_record})

    a = A({'id': 2, 'parent': 1}, db=db)
    a.mark_loaded()
    assert a.get('parent_id') == 1
    assert a.get('parent.name') == 'testing'


def test_model_custom_id_method():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()

        @id.getter()
        def get_id(self, **context):
            return 1

    a = A()
    assert a.id() == 1


def test_model_from_different_databases_not_equal(mock_db):
    import orb

    db_a = mock_db()
    db_b = mock_db()

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    a = A({'id': 1}, db=db_a)
    a.mark_loaded()

    b = A({'id': 1}, db=db_b)
    b.mark_loaded()

    assert a != b
    assert a.is_record()
    assert not a.is_record(db=db_b)


def test_model_attribute_iteration():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A({'id': 1, 'name': 'Testing'})
    attrs = dict(a.iter_attributes(a.schema().columns().values()))
    assert attrs == {'id': 1, 'name': 'Testing'}


def test_model_attribute_iteration_ignores_expansion():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()
        blocked = orb.StringColumn(flags={'RequiresExpand'})

    a = A({'id': 1, 'name': 'Testing', 'blocked': 'yes'})
    cols = a.schema().columns().values()

    # check without expanded
    attrs = dict(a.iter_attributes(cols))
    assert attrs == {'id': 1, 'name': 'Testing'}

    # check with expanded
    attrs = dict(a.iter_attributes(cols, tree={'blocked'}))
    assert attrs == {'id': 1, 'name': 'Testing', 'blocked': 'yes'}


def test_model_attribute_iteration_permission_denied():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()
        blocked = orb.StringColumn(flags={'Private'})

    a = A({'id': 1, 'name': 'Testing', 'blocked': 'yes'})
    cols = a.schema().columns().values()
    attrs = dict(a.iter_attributes(cols))
    assert attrs == {'id': 1, 'name': 'Testing'}


def test_model_attribute_iteration_with_virtual_columns():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()

        @orb.virtual(orb.StringColumn)
        def name(self, **context):
            return 'custom name'

    a = A({'id': 1})
    cols = A.schema().columns().values()
    attrs = dict(a.iter_attributes(cols))
    assert attrs == {'id': 1, 'name': 'custom name'}


def test_model_attribute_iteration_with_gettermethod():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

        @name.getter()
        def get_name(self, **context):
            return 'custom name'

    a = A({'id': 1, 'name': 'Testing'})
    cols = a.schema().columns().values()
    attrs = dict(a.iter_attributes(cols))
    assert attrs == {'id': 1, 'name': 'custom name'}


def test_model_attribute_iteration_with_i18n():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn(flags={'I18n'})

    cols = A.schema().columns().values()

    a = A({'id': 1, 'name': 'Testing'})
    attrs = dict(a.iter_attributes(cols))
    assert attrs == {'id': 1, 'name': 'Testing'}

    b = A({'id': 1, 'name': {'en_US': 'Testing', 'fr_FR': 'Testing2'}})
    attrs = dict(b.iter_attributes(cols))
    assert attrs == {'id': 1, 'name': 'Testing'}

    attrs = dict(b.iter_attributes(cols, context=orb.Context(locale='fr_FR')))
    assert attrs == {'id': 1, 'name': 'Testing2'}

    attrs = dict(b.iter_attributes(cols, context=orb.Context(locale='all')))
    assert attrs == {'id': 1, 'name': {'en_US': 'Testing', 'fr_FR': 'Testing2'}}


def test_model_attribute_iteration_with_references():
    import orb

    system = orb.System()

    class A(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        name = orb.StringColumn()

    class B(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        reference = orb.ReferenceColumn('A')

    a = A({'id': 1, 'name': 'a'})
    b = B({'id': 1, 'reference': a})

    cols = B.schema().columns().values()

    # test base attributes
    attrs = dict(b.iter_attributes(cols))
    assert attrs == {'id': 1, 'reference_id': 1}

    # test expanded reference
    attrs = dict(b.iter_attributes(cols, tree={'reference': {}}))
    assert attrs == {
        'id': 1,
        'reference_id': 1,
        'reference': a
    }

    # test expanded references as data
    attrs = dict(b.iter_attributes(cols, tree={'reference': {}}, context=orb.Context(returning='data')))
    assert attrs == {
        'id': 1,
        'reference_id': 1,
        'reference': {
            'id': 1,
            'name': 'a'
        }
    }


def test_model_tree_iteration():
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        parent = orb.ReferenceColumn('A')
        name = orb.StringColumn()

    a = A({'id': 1, 'name': 'a'})
    b = A({'id': 2, 'name': 'b', 'parent': a})
    c = A({'id': 3, 'name': 'c', 'parent': b})

    tree = {'parent': {}}
    attrs = dict(c.iter_expand_tree(tree))
    assert attrs == {'parent': b}

    attrs = dict(c.iter_expand_tree(tree, orb.Context(returning='data')))
    assert attrs == {
        'parent': {
            'id': 2,
            'name': 'b',
            'parent_id': 1
        }
    }

    attrs = dict(c.iter_expand_tree({'blah': {}}))
    assert attrs == {}


def test_model_iteration():
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        parent = orb.ReferenceColumn('A')
        name = orb.StringColumn()

        @orb.virtual(orb.ReferenceColumn, reference='A', flags={'RequiresExpand'})
        def myself(self, **context):
            return self

    a = A({'id': 1, 'name': 'a'})
    b = A({'id': 2, 'name': 'b', 'parent': a})

    attrs = dict(b.iter_record(expand='parent'))
    assert attrs == {
        'id': 2,
        'name': 'b',
        'parent_id': 1,
        'parent': a
    }

    attrs = dict(b.iter_record(expand='myself'))
    assert attrs == {
        'id': 2,
        'name': 'b',
        'parent_id': 1,
        'myself': b
    }


def test_record_on_change_event():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    checks = {}

    def mark_changed(sender, event=None):
        checks['changed'] = True

    a = A()
    a.changed.connect(mark_changed, sender=a)

    a.set('name', 'testing')

    assert a.get('name') == 'testing'
    assert checks['changed']


def test_model_on_change_event():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    changes = set()

    def mark_changed(sender, event=None):
        changes.add(event.record)

    A.changed.connect(mark_changed, sender=A)

    a = A()
    b = A()

    a.set('name', 'a')
    b.set('name', 'b')

    assert a.get('name') == 'a'
    assert b.get('name') == 'b'
    assert a in changes
    assert b in changes


def test_model_on_delete_event(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A({'id': 1}, db=mock_db())
    a.mark_loaded()

    changes = {}
    def deleted(sender, event=None):
        changes['deleted'] = True

    a.deleted.connect(deleted, sender=a)
    a.delete()
    assert changes['deleted'] == True


def test_model_on_save_events(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    db = mock_db(responses={
        'insert': ([{
            'id': 1,
            'name': 'Testing'
        }], 0)
    })

    checks = {}

    def presave(sender, event=None):
        checks['presave'] = True

    def postsave(sender, event=None):
        checks['postsave'] = True

    a = A({'name': 'Testing'}, db=db)
    a.about_to_save.connect(presave, sender=a)
    a.saved.connect(postsave, sender=a)

    assert a.save() is True
    assert a.id() == 1
    assert checks['presave'] is True
    assert checks['postsave'] is True


def test_model_save(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    db = mock_db(responses={
        'insert': ([{
            'id': 1,
            'name': 'Testing'
        }], 0)
    })

    a = A({'name': 'Testing'}, db=db)

    assert a.save() is True
    assert a.id() == 1


def test_model_save_after_another(mock_db):
    import orb

    checks = set()

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

        def save(self, values=None, after=None, before=None, **context):
            if after is None:
                checks.add(self)
            return super(A, self).save(values=values,
                                       after=after,
                                       before=before,
                                       **context)


    db = mock_db(responses={
        'insert': [([{
            'id': 1,
            'name': 'Testing'
        }], 1), ([{
            'id': 2,
            'name': 'Testing'
        }], 1)]
    })

    a = A({'name': 'Testing'}, db=db)
    b = A({'name': 'Testing2'}, db=db)

    a.save(after=b)

    assert not a.is_record()
    assert not b.is_record()

    assert b.save() is True

    assert checks == {a, b}

    assert b.is_record()
    assert b.id() == 1

    assert a.is_record()
    assert a.id() == 2

    a.set('id', None)

    b.save()

    assert not a.is_record()


def test_model_save_before_another(mock_db):
    import orb

    checks = set()

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

        def save(self, values=None, after=None, before=None, **context):
            if before is None:
                checks.add(self)
            return super(A, self).save(values=values,
                                       after=after,
                                       before=before,
                                       **context)


    db = mock_db(responses={
        'insert': [([{
            'id': 1,
            'name': 'Testing'
        }], 1), ([{
            'id': 2,
            'name': 'Testing'
        }], 1)]
    })

    a = A({'name': 'Testing'}, db=db)
    b = A({'name': 'Testing2'}, db=db)

    a.save(before=b)

    assert not a.is_record()
    assert not b.is_record()

    assert b.save() is True

    assert checks == {a, b}

    assert b.is_record()
    assert b.id() == 2

    assert a.is_record()
    assert a.id() == 1

    a.set('id', None)

    b.save()

    assert not a.is_record()

def test_model_save_values(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        first_name = orb.StringColumn()
        last_name = orb.StringColumn()

    checks = {}

    def save_callback(sender, event=None):
        assert event.record.get('first_name') == 'Bob'
        assert event.record.get('last_name') == 'Jones'
        assert len(event.changes) == 1
        checks['saved'] = True

    db = mock_db()

    a = A({'id': 1, 'first_name': 'Tom', 'last_name': 'Jones'}, db=db)
    a.mark_loaded()

    a.about_to_save.connect(save_callback)
    a.save(values={'first_name': 'Bob'})

    assert checks['saved'] == True


def test_model_save_blocked():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        first_name = orb.StringColumn()
        last_name = orb.StringColumn()

    a = A({'id': 1, 'first_name': 'Bob', 'last_name': 'Dylan'})

    checks = {}
    def save_blocker(sender, event=None):
        checks['blocked'] = True
        event.prevent_default = True

    a.about_to_save.connect(save_blocker)

    assert a.save() is False
    assert not a.is_record()
    assert checks['blocked'] is True


def test_model_save_without_results(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A({'id': 1, 'name': 'testing'}, db=mock_db())

    assert a.save() is False
    assert not a.is_record()


def test_model_parsing():
    import orb

    class Test(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent = orb.ReferenceColumn('Test')

    a = Test()
    a.parse({'id': 1})
    assert a.get('id') == 1

    b = Test()
    b.parse({'tests.id': 1})
    assert a.get('id') == 1

    c = Test()
    c.parse({'tests.id': 1, 'not_tests.name': 'test'})
    assert b.get('id') == 1
    assert b.get('name') is None

    d = Test()
    d.parse({'tests.id': 1, 'parent_id': 2})
    assert d.get('id') == 1
    assert d.get('parent_id') == 2

    with pytest.raises(orb.errors.DatabaseNotFound):
        assert d.get('parent.id') is None

    from collections import OrderedDict as odict

    e = Test()
    e.parse(odict([('id', 1), ('parent', {'id': 2, 'name': 'test'}), ('parent_id', 2)]))
    assert e.get('id') == 1
    assert e.get('parent_id') == 2
    assert e.get('parent.id') == 2
    assert isinstance(e.get('parent'), Test)

    f = Test()
    f.parse({'id': 1, 'parent_id': 2, 'name': 'test', 'preload': 'tested'})
    assert f.get('id') == 1
    assert f.preloaded_data('preload') == 'tested'


def test_preload_controls():
    import orb

    class Test(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    a = Test()
    a.preload_data({'test': 1, 'test2': 2})
    assert a.preloaded_data('test') == 1
    assert a.preloaded_data('test2') == 2
    assert a.preloaded_data('test3') == None


def test_model_set_attribute_basic():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A()
    a.set('name', 'testing')

    assert a.get('name') == 'testing'


def test_model_set_attribute_with_settermethod():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

        @name.setter()
        def set_name(self, value, **context):
            return self.set('name', '[name] ' + value if value else value, use_method=False, **context)

    a = A()
    a.set('name', 'testing')
    assert a.get('name') == '[name] testing'


def test_model_set_attribute_with_i18n():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn(flags={'I18n'})

    a = A()
    a.set('name', 'testing')
    a.set('name', 'testing2', locale='fr_FR')

    assert a.get('name') == 'testing'
    assert a.get('name', locale='en_US') == 'testing'
    assert a.get('name', locale='fr_FR') == 'testing2'

    with orb.Context(locale='fr_FR'):
        assert a.get('name') == 'testing2'


def test_model_set_collection_data(mock_db):
    import orb

    system = orb.System()

    class A(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

    a = A()
    b = A()
    c = A()

    with orb.Context(db=mock_db(), system=system):
        a.set('children', [b, c])

    assert isinstance(a.get('children'), orb.Collection)
    assert len(a.get('children')) == 2

    assert b in a.get('children')
    assert c in a.get('children')

    assert b.get('parent') == a
    assert c.get('parent') == a


def test_model_set_collection_with_settermethod(mock_db):
    import orb

    checked = {}
    system = orb.System()

    class A(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

        @children.setter()
        def set_children(self, collection, **context):
            checked['setter'] = True
            self.set('children', [collection[0]], use_method=False, **context)

    a = A()
    b = A()
    c = A()

    with orb.Context(db=mock_db(), system=system):
        a.set('children', [b, c])

    assert checked['setter'] == True
    assert isinstance(a.get('children'), orb.Collection)
    assert len(a.get('children')) == 1

    assert b in a.get('children')
    assert c not in a.get('children')

    assert b.get('parent') == a
    assert c.get('parent') is None


def test_model_update_context():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A()
    assert a.context().locale == 'en_US'
    assert a.set_context({'locale': 'fr_FR'}) is True
    assert a.context().locale == 'fr_FR'
    assert a.set_context(orb.Context(columns=['id'])) is True
    assert a.context().columns == ['id']
    assert a.set_context(True) is False


def test_model_id_setter_method():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    a = A()
    assert a.id() is None
    assert a.set_id(1) is True
    assert a.id() == 1


def test_model_id_setter_method_with_custom_id_column():
    import orb

    class A(orb.Table):
        __register__ = False
        __id__ = 'name'

        name = orb.StringColumn()

    a = A()
    assert a.id() is None
    assert a.set_id('testing') is True
    assert a.id() == 'testing'


def test_model_id_setter_method_with_custom_method():
    import orb

    class A(orb.Table):
        __register__ = False
        __id__ = 'name'

        name = orb.StringColumn()

        @name.setter()
        def set_name(self, name, **context):
            self.set('name', '[name] ' + name if name else name, use_method=False, **context)
            return True

    a = A()
    assert a.id() is None
    assert a.set_id('testing') is True
    assert a.id() == '[name] testing'
    assert a.get('name') == '[name] testing'


def test_model_update_values(mock_db):
    import orb

    system = orb.System()

    class A(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

    a = A()
    b = A()
    c = A()

    with orb.Context(db=mock_db(), system=system):
        c.update({'id': 1, 'name': 'testing', 'parent': a, 'children': [b]})

    assert c.get('id') == 1
    assert c.get('name') == 'testing'
    assert c.get('parent') == a
    assert b in c.get('children')


def test_model_validation():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        first_name = orb.StringColumn(flags={'Required'})
        last_name = orb.StringColumn(flags={'Required'})

        by_first_and_last_name = orb.Index(['first_name', 'last_name'])

    a = A({'first_name': 'john', 'last_name': 'doe'})
    assert a.validate()

    b = A({'first_name': 'john'})
    with pytest.raises(orb.errors.ValidationError):
        assert b.validate()

    assert b.validate(columns=['first_name'])


def test_model_select_all_records():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    records = A.all(limit=10)
    assert isinstance(records, orb.Collection)
    assert records.context().limit is None


def test_model_select_with_base_query():
    import orb

    class A(orb.Table):
        __register__ = False
        __base_query__ = orb.Query('active') == True

        id = orb.IdColumn()
        active = orb.BooleanColumn()

    result = {
        'value': True,
        'caseSensitive': False,
        'functions': [],
        'inverted': False,
        'model': '',
        'math': [],
        'type': 'query',
        'op': 'Is',
        'column': 'active'
    }
    assert A.get_base_query().__json__() == result


def test_model_with_custom_collection_type():
    import orb

    class TestCollection(orb.Collection):
        pass

    class Test(orb.Table):
        __collection_type__ = TestCollection

    assert Test.get_collection_type() == TestCollection
    assert isinstance(Test.all(), TestCollection)


def test_model_creation(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn()

    with orb.Context(db=mock_db(responses={'insert': ([{'id': 1}], 1)})):
        a = A.create({'name': 'testing'})

    assert isinstance(a, A)
    assert a.get('id') == 1
    assert a.get('name') == 'testing'


def test_model_creation_with_collections(mock_db):
    import orb

    system = orb.System()

    class A(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        name = orb.StringColumn()
        parent = orb.ReferenceColumn('A')
        children = orb.ReverseLookup('A.parent')

    a = A()
    b = A()

    with orb.Context(db=mock_db(), system=system):
        c = A.create({'name': 'test', 'children': [a, b]})

    assert c.get('name') == 'test'
    assert a in c.get('children')
    assert b in c.get('children')
    assert a.get('parent') is c
    assert b.get('parent') is c


def test_polymorphic_model_creation(mock_db):
    import orb

    system = orb.System()

    class Fruit(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        class_type = orb.StringColumn(flags={'Polymorphic'})

    class Apple(Fruit):
        __system__ = system

        name = orb.StringColumn()

    with orb.Context(db=mock_db()):
        a = Fruit.create({'class_type': 'Apple', 'name': 'GrannySmith'})
        b = Fruit.create({'class_type': 'Fruit'})

    assert isinstance(a, Apple)
    assert isinstance(b, Fruit) and not isinstance(b, Apple)


def test_model_ensure_exists(mock_db):
    import orb

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn()
        first_name = orb.StringColumn()
        last_name = orb.StringColumn()

    with orb.Context(db=mock_db()):
        u = User.ensure_exists({'username': 'jdoe'}, {'first_name': 'John', 'last_name': 'Doe'})

    assert u.get('username') == 'jdoe'
    assert u.get('first_name') == 'John'
    assert u.get('last_name') == 'Doe'


def test_model_ensure_exists_with_virtual_columns(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        first_name = orb.StringColumn()
        last_name = orb.StringColumn()

        @orb.virtual(orb.StringColumn)
        def display_name(self, **context):
            return '{0} {1}'.format(self.get('first_name'), self.get('last_name'))

        @display_name.setter()
        def set_display_name(self, display_name, **context):
            first_name, last_name = display_name.split(' ', 1)
            self.set('first_name', first_name)
            self.set('last_name', last_name)

    with orb.Context(db=mock_db()):
        a = A.ensure_exists({'first_name': 'john', 'last_name': 'doe'})
        b = A.ensure_exists({'display_name': 'john doe'})

    assert a.get('display_name') == 'john doe'
    assert b.get('first_name') == 'john'
    assert b.get('last_name') == 'doe'


def test_model_ensure_exists_with_reference(mock_db):
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        parent = orb.ReferenceColumn('A')

    with orb.Context(db=mock_db()):
        a = A.ensure_exists({'parent': 1})

    assert a.get('parent_id') == 1


def test_model_ensure_exists_requires_values():
    import orb

    class A(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()

    with pytest.raises(orb.errors.OrbError):
        assert A.ensure_exists({}) is None


def test_model_initialization_with_keyable_id(mock_db):
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn(flags={'Keyable'})

    class B(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn()

    with orb.Context(db=mock_db()):
        assert A.fetch(1) is None
        assert A.fetch('jdoe') is None
        assert B.fetch('jdoe') is None


def test_model_on_sync(mock_db):
    import orb

    system = orb.System()
    db = mock_db()
    db.set_system(system)

    synced = set()

    class A(orb.Table):
        __system__ = system

        id = orb.IdColumn()

        @classmethod
        def on_sync(cls, event):
            synced.add(A)
            return super(A, cls).on_sync(event)

    class B(orb.Table):
        __system__ = system

        id = orb.IdColumn()

    def test_sync_event(sender, event=None):
        synced.add(sender)

    B.synced.connect(test_sync_event, sender=B)

    db.sync()

    assert A in synced
    assert B in synced


def test_model_restore_record():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        first_name = orb.StringColumn()
        last_name = orb.StringColumn()

    a = A.restore_record({'id': 1, 'first_name': 'john', 'last_name': 'doe'})
    assert a.is_record()
    assert a.is_loaded()
    assert a.get('first_name') == 'john'
    assert a.get('last_name') == 'doe'


def test_model_restore_record_from_self():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    a = A()
    b = A.restore_record(a)
    assert b is a


def test_model_restore_polymorphic():
    import orb

    class Fruit(orb.Table):
        __system__ = orb.System()

        id = orb.IdColumn()
        class_type = orb.StringColumn(flags={'Polymorphic'})

    class Apple(Fruit):
        name = orb.StringColumn()

    a = Fruit.restore_record({'id': 1, 'class_type': 'Apple', 'name': 'GrannySmith'})
    assert isinstance(a, Apple)


def test_model_search():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn(flags={'Searchable'})

    records = A.search('testing')
    assert isinstance(records, orb.Collection)
    assert records.context().where.__json__() == {
        'caseSensitive': False,
        'column': 'name',
        'functions': ['AsString'],
        'inverted': False,
        'math': [],
        'model': 'A',
        'op': 'Matches',
        'type': 'query',
        'value': u'(^|.*\\s)testing'
    }


def test_model_search_with_querying():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn(flags={'Searchable'})

    records = A.search('testing', where=orb.Query('id') != 1)
    query = records.context().where.__json__()

    validation_query = {
        'op': 'And',
        'queries': [{
            'caseSensitive': False,
            'column': 'id',
            'functions': [],
            'inverted': False,
            'math': [],
            'model': '',
            'op': 'IsNot',
            'type': 'query',
            'value': 1
        }, {
            'caseSensitive': False,
            'column': 'name',
            'functions': ['AsString'],
            'inverted': False,
            'math': [],
            'model': 'A',
            'op': 'Matches',
            'type': 'query',
            'value': u'(^|.*\\s)testing'
        }],
        'type': 'compound'
    }

    assert isinstance(records, orb.Collection)
    assert query == validation_query


def test_model_search_engine():
    import orb

    class A(orb.Table):
        __register__ = False

        id = orb.IdColumn()

    assert isinstance(A.get_search_engine(), orb.AbstractSearchEngine)


def test_view_not_editable():
    import orb

    class A(orb.View):
        __register__ = False

    a = A()
    with pytest.raises(orb.errors.OrbError):
        assert a.save() is None

    with pytest.raises(orb.errors.OrbError):
        assert a.delete() is None


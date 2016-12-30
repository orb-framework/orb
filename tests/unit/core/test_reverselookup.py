import pytest


@pytest.fixture()
def mock_lookup_schema():
    import orb

    system = orb.System()

    class Source(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        targets = orb.ReverseLookup('Target.source')

    class Target(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        name = orb.StringColumn()
        source = orb.ReferenceColumn('Source')

    return (Source, Target)


def test_lookup_initialization(mock_lookup_schema):
    Source, Target = mock_lookup_schema

    targets = Source.schema().collector('targets')

    assert targets.model() is Target
    assert targets.column() is Target.schema().column('source')


def test_lookup_duplication(mock_lookup_schema):
    Source, _ = mock_lookup_schema

    targets_a = Source.schema().collector('targets')
    targets_b = targets_a.copy()

    assert targets_a.model() is targets_b.model()
    assert targets_a.column() is targets_b.column()
    assert targets_a.remove_action() is targets_b.remove_action()


def test_lookup_json_export(mock_lookup_schema):
    Source = mock_lookup_schema[0]

    targets = Source.schema().collector('targets')
    jdata = targets.__json__()

    assert jdata['model'] == 'Target'
    assert jdata['target'] == 'source_id'


def test_lookup_add_record(mock_lookup_schema, mock_db):
    import orb

    Source, Target = mock_lookup_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})
    b = Target({'id': 1})

    with orb.Context(db=mock_db()):
        r = targets.add_record(a, b)

    assert r is b
    assert b.get('source') is a


def test_lookup_add_record_with_invalid_type(mock_lookup_schema, mock_db):
    import orb

    Source, Target = mock_lookup_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})

    with pytest.raises(orb.errors.ValidationError):
        assert targets.add_record(a, a) is None


def test_lookup_create_record_via_id(mock_lookup_schema, mock_db):
    import orb

    Source, Target = mock_lookup_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})

    with orb.Context(db=mock_db()):
        r = targets.create_record(a, {'id': 2})

    assert isinstance(r, Target)
    assert r.get('id') == 2
    assert r.get('source') is a


def test_lookup_create_record_via_reference(mock_lookup_schema, mock_db):
    import orb

    Source, Target = mock_lookup_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})
    b = Target({'id': 2})

    with orb.Context(db=mock_db()):
        r = targets.create_record(a, b)

    assert isinstance(r, Target)
    assert r is b
    assert r.get('id') == 2
    assert r.get('source') is a


def test_lookup_collection(mock_lookup_schema,):
    Source, Target = mock_lookup_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})
    b = Source()

    records = targets.collect(a)
    records_b = targets.collect(b)

    jdata = records.context().where.__json__()

    import pprint
    pprint.pprint(jdata)

    query_json = {
        'case_sensitive': False,
        'column': 'source',
        'deltas': [],
        'inverted': False,
        'model': 'Target',
        'op': 'Is',
        'type': 'query',
        'value': {
            'id': 1
        }
    }

    assert records_b.is_null()

    assert not records.is_null()
    assert records.bound_source_record() is a
    assert records.bound_collector() is targets
    assert jdata == query_json


def test_lookup_collect_expand(mock_lookup_schema, mock_db):
    import orb

    Source, Target = mock_lookup_schema
    targets = Source.schema().collector('targets')

    q = orb.Query('targets.name') == 'testing'

    with orb.Context(db=mock_db()):
        expanded = targets.collect_expand(q, ['targets', 'name'])
        jdata = expanded.context().where.__json__()

    results = {
        'case_sensitive': False,
        'column': 'name',
        'deltas': [],
        'inverted': False,
        'model': 'Target',
        'op': 'Is',
        'type': 'query',
        'value': u'testing'
    }

    assert isinstance(expanded, orb.Collection)
    assert expanded.bound_model() is Target
    assert expanded.context().columns == [Target.schema().column('source')]
    assert jdata == results


def test_lookup_delete_records_via_unset(mock_lookup_schema, mock_db):
    import orb

    Source, Target = mock_lookup_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})
    b = Target({'id': 1, 'source': a})

    a.mark_loaded()
    b.mark_loaded()

    with orb.Context(db=mock_db()):
        assert targets.delete_records(a, [b]) == 1

    with orb.Context(db=mock_db()):
        assert targets.delete_records(a, []) == 0

    with orb.Context(db=mock_db()):
        assert targets.delete_records(Source(), [a,b]) == 0

    assert b.is_record()
    assert b.get('source') is None


def test_lookup_delete_records_via_delete(mock_lookup_schema, mock_db):
    import orb

    Source, Target = mock_lookup_schema
    targets = Source.schema().collector('targets')
    targets.set_remove_action('Delete')

    checks = set()
    def delete_check(sender, event=None):
        checks.add(sender)

    a = Source({'id': 1})
    b = Target({'id': 1, 'source': a})
    b.deleted.connect(delete_check, sender=b)

    a.mark_loaded()
    b.mark_loaded()

    with orb.Context(db=mock_db(responses={'delete': (tuple(), 1)})):
        assert targets.delete_records(a, [b]) == 1

    with orb.Context(db=mock_db()):
        assert targets.delete_records(a, []) == 0

    with orb.Context(db=mock_db()):
        assert targets.delete_records(Source(), [a,b]) == 0

    assert a.is_record()
    assert not b.is_record()
    assert a not in checks
    assert b in checks


def test_lookup_remove_record_via_unset(mock_lookup_schema, mock_db):
    import orb

    Source, Target = mock_lookup_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})
    b = Target({'id': 1, 'source': a})

    a.mark_loaded()
    b.mark_loaded()

    with orb.Context(db=mock_db()):
        assert targets.remove_record(a, a) == 0
        assert targets.remove_record(a, b) == 1

    assert b.is_record()
    assert b.get('source') is None


def test_lookup_remove_record_via_delete(mock_lookup_schema, mock_db):
    import orb

    Source, Target = mock_lookup_schema
    targets = Source.schema().collector('targets')
    targets.set_remove_action('Delete')

    checks = set()
    def delete_check(sender, event=None):
        checks.add(sender)

    a = Source({'id': 1})
    b = Target({'id': 1, 'source': a})
    b.deleted.connect(delete_check, sender=b)

    a.mark_loaded()
    b.mark_loaded()

    with orb.Context(db=mock_db(responses={'delete': (tuple(), 1)})):
        assert targets.remove_record(a, a) == 0
        assert targets.remove_record(a, b) == 1

    assert a.is_record()
    assert not b.is_record()
    assert a not in checks
    assert b in checks


def test_lookup_update_records_via_unset(mock_lookup_schema, mock_db):
    import orb

    Source, Target = mock_lookup_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})
    b = Source()
    c = Target({'id': 2})

    a.mark_loaded()
    c.mark_loaded()

    checks = set()

    def get_record(model, *args, **kw):
        if issubclass(model, Target):
            checks.add('found target')
            return [{
                'id': 1,
                'source_id': 1,
                'name': 'testing'
            }]
        else:
            return []

    # test updating with a record
    with orb.Context(db=mock_db(responses={'select': get_record})):
        targets.update_records(a, [c])

    assert c.get('source') is a
    assert 'found target' in checks

    # test delayed update until a record exists
    with orb.Context(db=mock_db()):
        targets.update_records(b, [c])

    assert c.get('source') is b


def test_lookup_update_records_via_delete(mock_lookup_schema, mock_db):
    import orb

    Source, Target = mock_lookup_schema
    targets = Source.schema().collector('targets')
    targets.set_remove_action('Delete')

    a = Source({'id': 1})
    b = Source()
    c = Target({'id': 2})

    a.mark_loaded()
    c.mark_loaded()

    checks = set()

    def get_record(model, *args, **kw):
        if issubclass(model, Target):
            checks.add('found target')
            return [{
                'id': 1,
                'source_id': 1,
                'name': 'testing'
            }]
        else:
            return []

    # test updating with a record
    with orb.Context(db=mock_db(responses={'select': get_record})):
        targets.update_records(a, [c])

    assert c.get('source') is a
    assert 'found target' in checks

    # test delayed update until a record exists
    with orb.Context(db=mock_db()):
        targets.update_records(b, [c])

    assert c.get('source') is b

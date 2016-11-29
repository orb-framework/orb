import pytest


@pytest.fixture()
def mock_pipe_schema():
    import orb

    system = orb.System()

    class Source(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        targets = orb.Pipe('Through.source.target')

    class Through(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        source = orb.ReferenceColumn('Source')
        target = orb.ReferenceColumn('Target', field='fkey_target_target_id', alias='target_id')

    class Target(orb.Table):
        __system__ = system

        id = orb.IdColumn()
        name = orb.StringColumn()
        sources = orb.Pipe(through_model='Through',
                           from_column='target',
                           to_column='source',
                           model='Source')

    return (Source, Through, Target)


def test_pipe_initialization(mock_pipe_schema):
    Source, Through, Target = mock_pipe_schema

    targets = Source.schema().collector('targets')
    sources = Target.schema().collector('sources')

    assert targets.from_model() is Source
    assert targets.through_model() is Through
    assert targets.model() is Target
    assert targets.from_column() is Through.schema().column('source')
    assert targets.to_column() is Through.schema().column('target')

    assert sources.from_model() is Target
    assert sources.through_model() is Through
    assert sources.model() is Source
    assert sources.from_column() is Through.schema().column('target')
    assert sources.to_column() is Through.schema().column('source')


def test_pipe_duplication(mock_pipe_schema):
    Source, _, _ = mock_pipe_schema

    targets_a = Source.schema().collector('targets')
    targets_b = targets_a.copy()

    assert targets_a.from_model() is targets_b.from_model()
    assert targets_a.through_model() is targets_b.through_model()
    assert targets_a.model() is targets_b.model()
    assert targets_a.from_column() is targets_b.from_column()
    assert targets_a.to_column() is targets_b.to_column()


def test_pipe_json_export(mock_pipe_schema):
    Source = mock_pipe_schema[0]

    targets = Source.schema().collector('targets')
    jdata = targets.__json__()

    assert jdata['model'] == 'Target'
    assert jdata['through'] == 'Through'
    assert jdata['from'] == 'source_id'
    assert jdata['to'] == 'target_id'


def test_pipe_add_record(mock_pipe_schema, mock_db):
    import orb

    Source, Through, Target = mock_pipe_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})
    b = Target({'id': 1})

    with orb.Context(db=mock_db()):
        r = targets.add_record(a, b)

    assert isinstance(r, Through)
    assert r.get('source') is a
    assert r.get('target') is b


def test_pipe_create_record_via_id(mock_pipe_schema, mock_db):
    import orb

    Source, Through, Target = mock_pipe_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})

    with orb.Context(db=mock_db()):
        r = targets.create_record(a, {'id': 2})

    assert isinstance(r, Target)
    assert r.get('id') == 2


def test_pipe_create_record_via_reference(mock_pipe_schema, mock_db):
    import orb

    Source, Through, Target = mock_pipe_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})
    b = Target({'id': 2})

    with orb.Context(db=mock_db()):
        r = targets.create_record(a, b)

    assert isinstance(r, Target)
    assert r.get('id') == 2
    assert r is b


def test_pipe_create_record_via_reference_value(mock_pipe_schema, mock_db):
    import orb

    Source, Through, Target = mock_pipe_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})

    with orb.Context(db=mock_db()):
        name = targets.create_record(a, {'target': {'id': 2}})
        alias = targets.create_record(a, {'target_id': {'id': 2}})
        field = targets.create_record(a, {'fkey_target_target_id': {'id': 2}})

    assert isinstance(name, Target)
    assert isinstance(alias, Target)
    assert isinstance(field, Target)
    assert name.get('id') == alias.get('id') == field.get('id') == 2


def test_pipe_collection(mock_pipe_schema,):
    Source, Through, Target = mock_pipe_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})
    b = Source()

    records = targets.collect(a)
    records_b = targets.collect(b)

    query_json = {
        'op': 'And',
        'queries': [{
            'case_sensitive': False,
            'column': 'id',
            'functions': [],
            'inverted': False,
            'math': [],
            'model': 'Target',
            'op': 'Is',
            'type': 'query',
            'value': {
                'case_sensitive': False,
                'column': 'target',
                'functions': [],
                'inverted': False,
                'math': [],
                'model': 'Through',
                'op': 'Is',
                'type': 'query',
                'value': None
            }
        }, {
            'case_sensitive': False,
            'column': 'source',
            'functions': [],
            'inverted': False,
            'math': [],
            'model': 'Through',
            'op': 'Is',
            'type': 'query',
            'value': {
                'id': 1
            }
        }],
        'type': 'compound'
    }

    assert records_b.is_null()

    assert not records.is_null()
    assert records.bound_source_record() is a
    assert records.bound_collector() is targets
    assert records.context().where.__json__() == query_json


def test_pipe_collect_expand(mock_pipe_schema, mock_db):
    import orb

    Source, Through, Target = mock_pipe_schema
    targets = Source.schema().collector('targets')

    q = orb.Query('targets.name') == 'testing'

    with orb.Context(db=mock_db()):
        expanded = targets.collect_expand(q, ['targets', 'name'])
        jdata = expanded.context().where.__json__()

    results = {
        'case_sensitive': False,
        'column': 'target',
        'functions': [],
        'inverted': False,
        'math': [],
        'model': 'Through',
        'op': 'IsIn',
        'type': 'query',
        'value': []
    }

    assert isinstance(expanded, orb.Collection)
    assert expanded.bound_model() is Through
    assert jdata == results


def test_pipe_delete_records(mock_pipe_schema, mock_db):
    import orb

    Source, Through, Target = mock_pipe_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})
    b = Target({'id': 1})

    def get_record(model, *args, **kw):
        if issubclass(model, Through):
            return [{
                'id': 1,
                'source_id': 1,
                'target_id': 1
            }]
        else:
            return []

    with orb.Context(db=mock_db(responses={'select': get_record, 'delete': [([], 1)]})):
        assert targets.delete_records(a, [b]) == 1

    with orb.Context(db=mock_db()):
        assert targets.delete_records(a, []) == 0


def test_pipe_remove_record(mock_pipe_schema, mock_db):
    import orb

    Source, Through, Target = mock_pipe_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})
    b = Target({'id': 1})

    def get_record(model, *args, **kw):
        if issubclass(model, Through):
            return [{
                'id': 1,
                'source_id': 1,
                'target_id': 1
            }]
        else:
            return []

    with orb.Context(db=mock_db(responses={'select': get_record, 'delete': [([], 1)]})):
        assert targets.remove_record(a, b) == 1

    with orb.Context(db=mock_db()):
        assert targets.remove_record(a, b) == 0


def test_pipe_update_records(mock_pipe_schema, mock_db):
    import orb

    Source, Through, Target = mock_pipe_schema
    targets = Source.schema().collector('targets')

    a = Source({'id': 1})
    b = Source()
    c = Target({'id': 2})

    a.mark_loaded()

    # test updating with a record
    with orb.Context(db=mock_db()):
        targets.update_records(a, [c])

    # test delayed update until a record exists
    with orb.Context(db=mock_db()):
        targets.update_records(b, [c])
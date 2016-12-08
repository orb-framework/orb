def test_simple_search_engine():
    import orb

    engine = orb.SearchEngine.factory('simple')

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn(flags={'Searchable'})

    collection = engine.search(User, 'jdoe')

    jquery = collection.context().where.__json__()

    valid_query = {
        'case_sensitive': False,
        'column': 'username',
        'functions': ['AsString'],
        'inverted': False,
        'math': [],
        'model': 'User',
        'op': 'Matches',
        'type': 'query',
        'value': u'(^|.*\\s)jdoe'
    }

    import pprint
    pprint.pprint(jquery)

    assert jquery == valid_query


def test_simple_search_engine_requires_searchable_columns():
    import orb

    engine = orb.SearchEngine.factory('simple')

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn()

    assert engine.search(User, 'jdoe').is_null()


def test_simple_search_engine_with_specific_columns():
    import orb

    engine = orb.SearchEngine.factory('simple')

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn(flags={'Searchable'})

    collection = engine.search(User, 'doe username:john')

    jquery = collection.context().where.__json__()

    valid_query = {'op': 'And',
        'queries': [{'case_sensitive': False,
                  'column': 'username',
                  'functions': ['AsString'],
                  'inverted': False,
                  'math': [],
                  'model': 'User',
                  'op': 'Matches',
                  'type': 'query',
                  'value': u'(^|.*\\s)doe'},
                 {'case_sensitive': False,
                  'column': 'username',
                  'functions': [],
                  'inverted': False,
                  'math': [],
                  'model': 'User',
                  'op': 'Is',
                  'type': 'query',
                  'value': 'john'}],
        'type': 'compound'}

    import pprint
    pprint.pprint(jquery)

    assert jquery == valid_query


def test_simple_search_engine_with_exact_matches():
    import orb

    engine = orb.SearchEngine.factory('simple')

    class User(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        username = orb.StringColumn(flags={'Searchable'})

    collection = engine.search(User, '"john doe"')

    jquery = collection.context().where.__json__()

    valid_query = {'case_sensitive': False,
        'column': 'username',
        'functions': ['AsString'],
        'inverted': False,
        'math': [],
        'model': 'User',
        'op': 'Matches',
        'type': 'query',
        'value': u'(^|.*\\s)john\\ doe'}

    import pprint
    pprint.pprint(jquery)

    assert jquery == valid_query

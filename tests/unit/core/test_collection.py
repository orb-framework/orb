import pytest

# define module level fixtures
# -------------


@pytest.fixture()
def mock_user_data():
    return {
        'username': 'jdoe',
        'first_name': 'John',
        'last_name': 'Doe'
    }


@pytest.fixture()
def mock_user_collection(mock_user_data, MockUser):
    from orb.core.collection import Collection

    def _gen_list(count=0, **context):
        if count > 0:
            records = [MockUser(mock_user_data) for _ in range(count)]
        else:
            records = None
        return Collection(records=records, model=MockUser, **context)
    return _gen_list


# define tests
# -------------


def test_batch_iterator(mock_user_collection):
    from orb.core.collection import BatchIterator

    collection = mock_user_collection(100)
    batch_iter = BatchIterator(collection, batch=10)

    count = 0
    for _ in batch_iter:
        count += 1

    assert collection.page_count(page_size=10) == 10
    assert count == 100


def test_batch_iterator_from_helper_method(mock_user_collection):
    collection = mock_user_collection(100)

    count = 0
    for _ in collection.iter_batches(size=10):
        count += 1

    assert collection.page_count(page_size=10) == 10
    assert count == 100


# -------------


def test_collection_nonzero_check(mock_db, MockUser):
    from orb.core.collection import Collection

    non_zero_db = mock_db(responses={
        'count': 1
    })

    zero_db = mock_db(responses={
        'count': 0
    })

    a = Collection()
    b = Collection([])
    c = Collection(model=MockUser, db=zero_db)
    d = Collection(model=MockUser, db=non_zero_db)
    e = Collection([1, 2, 3])

    assert bool(a) is False
    assert bool(b) is False
    assert bool(c) is False
    assert bool(d) is True
    assert bool(e) is True

    assert a.is_null()
    assert not b.is_null()
    assert not c.is_null()
    assert not d.is_null()
    assert not e.is_null()


def test_create_collection_with_bound_model(mock_user_collection, MockUser):
    collection = mock_user_collection(3)
    assert collection.model() == MockUser


def test_basic_collection_serialization(mock_user_collection, mock_user_data, assert_dict_equals):
    collection = mock_user_collection(3)
    jdata = collection.__json__()

    assert isinstance(jdata, list)
    assert len(jdata) == 3
    assert_dict_equals(mock_user_data, jdata[0])


def test_collection_serialization_returning_count(mock_user_collection):
    collection = mock_user_collection(3, returning='count')
    jdata = collection.__json__()

    assert jdata == {'count': 3}


def test_collection_serialization_returning_ids(mock_user_collection):
    collection = mock_user_collection(3, returning='ids')
    jdata = collection.__json__()

    assert jdata == {'ids': [None, None, None]}


def test_collection_serialization_returning_first_record(mock_user_collection,
                                                         mock_user_data,
                                                         assert_dict_equals):
    collection = mock_user_collection(3, returning='first')
    jdata = collection.__json__()

    assert isinstance(jdata, dict)
    assert jdata.keys() == ['first']
    assert_dict_equals(mock_user_data, jdata['first'])


def test_collection_serialization_returning_last_record(mock_user_collection,
                                                        mock_user_data,
                                                        assert_dict_equals):
    collection = mock_user_collection(3, returning='last')
    jdata = collection.__json__()
    assert isinstance(jdata, dict)
    assert jdata.keys() == ['last']
    assert_dict_equals(mock_user_data, jdata['last'])


def test_collection_serialization_expand_combination(mock_user_collection,
                                                     mock_user_data,
                                                     assert_dict_equals):
    collection = mock_user_collection(3, expand='first,last,count,ids,records')
    jdata = collection.__json__()
    assert isinstance(jdata, dict)
    assert set(jdata.keys()) == {'first', 'last', 'count', 'ids', 'records'}
    assert jdata['count'] == 3
    assert jdata['ids'] == [None, None, None]
    assert jdata['first'] == jdata['last']
    assert_dict_equals(mock_user_data, jdata['first'])
    assert_dict_equals(mock_user_data, jdata['records'][0])


def test_collection_serialization_of_column_data(mock_user_data, MockUser, assert_dict_equals):
    from orb.core.collection import Collection

    collection = Collection(model=MockUser, returning='data', columns='username')
    collection.preload_data({'records': [mock_user_data]})
    data = collection.__json__()
    assert data[0] == {'username': 'jdoe'}

    collection = Collection(model=MockUser, returning='values', columns='username')
    collection.preload_data({'records': [mock_user_data]})
    data = collection.__json__()
    assert data[0] == 'jdoe'

    collection = Collection(model=MockUser, returning='values', columns='username,last_name')
    collection.preload_data({'records': [mock_user_data]})
    data = collection.__json__()
    assert data[0] == ('jdoe', 'Doe')

    collection = Collection(model=MockUser, returning='data')
    collection.preload_data({'records': [mock_user_data]})
    data = collection.__json__()
    assert_dict_equals(mock_user_data, data[0])


def test_collection_returning_data(mock_user_data, MockUser):
    from orb.core.collection import Collection

    collection = Collection(model=MockUser, returning='data', columns='username')
    collection.preload_data({'records': [mock_user_data]})
    data = collection.records()
    assert data[0] == {'username': 'jdoe'}


def test_collection_length(mock_user_collection):
    collection = mock_user_collection(3)
    assert len(collection) == 3


def test_iterate_over_null_collection():
    from orb.core.collection import Collection
    collection = Collection()
    count = 0
    for _ in collection:
        count += 1
    assert count == 0
    assert collection.is_null()


def test_fetch_records_for_iteration_from_backend(mock_db, MockUser, mock_user_data):
    from orb.core.collection import Collection

    db = mock_db(responses={
        'select': (mock_user_data for _ in xrange(3))
    })

    collection = Collection(model=MockUser, db=db)
    count = 0
    for record in collection:
        assert isinstance(record, MockUser)
        count += 1
    assert count == 3


def test_get_record_by_index(mock_user_collection, MockUser):
    collection = mock_user_collection(3)

    assert isinstance(collection[0], MockUser)
    assert isinstance(collection[1], MockUser)
    assert isinstance(collection[2], MockUser)
    with pytest.raises(IndexError):
        collection[3]


def test_collection_slicing(mock_user_collection):
    collection = mock_user_collection(10)

    assert len(collection) == 10
    assert len(collection[2:4]) == 2
    assert len(collection[:2]) == 2
    assert len(collection[2:]) == 8


def test_collection_cache_addition(mock_user_collection, MockUser):
    collection = mock_user_collection(1)

    user = MockUser()
    collection.add(user)

    assert collection[-1] == user


def test_collection_cache_addition_to_empty_collection(mock_user_collection, MockUser):
    collection = mock_user_collection(0)

    assert not collection.is_null()

    user = MockUser()
    collection.add(user)

    assert collection[-1] == user


def test_collection_cache_addition_to_null_collection(MockUser):
    from orb.core.collection import Collection
    collection = Collection()

    assert collection.is_null()
    assert collection.model() is None

    user = MockUser()
    collection.add(user)

    assert collection[-1] == user


def test_collection_cache_addition_of_incorrect_model_to_collection(mock_user_collection,
                                                                    MockUser,
                                                                    MockGroup):
    collection = mock_user_collection(0)

    assert collection.model() == MockUser
    with pytest.raises(NotImplementedError):
        collection.add(MockGroup())


def test_collection_addition_through_collector(mock_user_collection, MockUser):
    # define a mock collector instance to validate
    # add route for a collection
    class MockCollector(object):
        def __init__(self, a, b):
            self.user_a = a
            self.user_b = b
            self.added = False

        def add_record(self, source_record, record, **context):
            assert source_record == self.user_a
            assert record == self.user_b
            self.added = True

    a = MockUser({'username': 'john.doe'})
    b = MockUser({'username': 'jane.doe'})

    collector = MockCollector(a, b)

    collection = mock_user_collection(0)
    collection.bind_collector(collector)
    collection.bind_source_record(a)
    collection.add(b)

    assert collector.added is True


def test_preloading_collection_data(mock_user_collection,
                                    mock_user_data,
                                    MockUser,
                                    assert_dict_equals):
    collection = mock_user_collection(0)
    collection.preload_data({'records': [mock_user_data]})

    assert len(collection) == 1
    assert isinstance(collection[0], MockUser)
    assert collection[0].get('username') == mock_user_data['username']


def test_accessing_a_record_using_the_at_method(mock_user_collection):
    collection = mock_user_collection(3)

    assert collection.at(0) == collection[0]
    assert collection.at(1) == collection[1]
    assert collection.at(2) == collection[2]
    assert collection.at(-1) is collection[-1]
    assert collection.at(3) is None


def test_create_record_from_collection(mock_user_collection,
                                       mock_user_data,
                                       MockUser):
    result = {}

    def create_function(model, values, **context):
        result['created'] = True
        return MockUser(values)

    MockUser.create = classmethod(create_function)

    collection = mock_user_collection(0)
    collection.create(mock_user_data)

    assert len(collection) == 1
    assert collection[0].get('username') == mock_user_data['username']
    assert result['created'] is True


def test_create_record_from_collectionr_using_collector(mock_user_collection,
                                                        mock_user_data,
                                                        MockUser):
    # define a mock collector instance to validate
    # add route for a collection
    class MockCollector(object):
        def __init__(self, a):
            self.user_a = a
            self.user_b = None

        def create_record(self, source_record, values, **context):
            self.user_b = MockUser(values)
            assert source_record == self.user_a
            return self.user_b

    user = MockUser({'username': 'john.doe'})
    collector = MockCollector(user)

    collection = mock_user_collection(0)
    collection.bind_collector(collector)
    collection.bind_source_record(user)

    record = collection.create(mock_user_data)
    assert collector.user_b is not None
    assert collector.user_b == record
    assert collector.user_b.get('username') == mock_user_data['username']


def test_copy_collection_with_collector_and_record(mock_user_collection,
                                                   MockUser):
    class MockCollector(object):
        pass

    collector = MockCollector()
    user = MockUser()

    collection = mock_user_collection(0)
    collection.bind_collector(collector).bind_source_record(user)

    coll_copy = collection.copy()
    assert coll_copy._Collection__bound_collector == collector
    assert coll_copy._Collection__bound_source_record == user


def test_copy_collection_with_preloaded_data(mock_user_collection,
                                             mock_user_data):
    collection = mock_user_collection(0)
    collection.preload_data({'records': [mock_user_data]})
    coll_copy = collection.copy()
    assert len(coll_copy) == 1


def test_null_collection_length_is_zero():
    from orb.core.collection import Collection

    collection = Collection()
    assert len(collection) == 0


def test_collection_with_preloaded_count():
    from orb.core.collection import Collection

    collection = Collection()
    collection.preload_data({'count': 3})

    assert len(collection) == 3


def test_collection_count_with_preloaded_records(mock_user_data):
    from orb.core.collection import Collection

    collection = Collection()
    collection.preload_data({'records': [mock_user_data]})

    assert len(collection) == 1


def test_collection_count_from_backend(mock_db, mock_user_collection):
    db = mock_db(responses={
        'count': 3
    })

    collection = mock_user_collection(0, db=db)
    assert len(collection) == 3


def test_collection_delete_records(mock_db, mock_user_collection):
    def delete_method(records, context):
        return None, len(records)

    db = mock_db(responses={
        'delete': delete_method
    })

    collection = mock_user_collection(3, db=db)
    assert collection.delete() == 3


def test_collection_delete_records_from_collector(mock_user_collection):
    from orb.core.collector import Collector
    from orb.core.collection import Collection

    class MockCollector(object):
        def __init__(self):
            self.deleted = False

        def delete_records(self, source_record, collection, **context):
            self.deleted = True
            return len(collection)

    collection = mock_user_collection(3)
    collector = MockCollector()

    collection.bind_collector(collector)

    assert collection.delete() == 3
    assert collector.deleted is True

    # validate that deleting with an abstract collector does not raise an error
    collection = Collection([])
    collection.bind_collector(Collector())
    assert collection.delete() == 0


def test_distinct_value_lookup(mock_db, mock_user_collection):
    db = mock_db(responses={
        'select': ({'username': 'jdoe'},)
    })

    collection = mock_user_collection(3, db=db)
    usernames = collection.distinct('username')
    assert usernames == ['jdoe']


def test_emptying_collection(mock_user_collection):
    from orb.core.collection import Collection

    null_collection = Collection()
    null_collection.empty()
    assert len(null_collection) == 0

    collection = mock_user_collection(3)
    collection.empty()
    assert len(collection) == 0


def test_retrieve_first_record_from_null_collection():
    from orb.core.collection import Collection

    null_collection = Collection()
    assert null_collection.first() is None


def test_retrieve_first_record_from_empty_collection():
    from orb.core.collection import Collection

    collection = Collection([])

    assert collection.is_empty() is True
    assert collection.is_null() is False
    assert collection.first() is None


def test_retrieve_first_record_from_preloaded_cache(mock_user_data, MockUser):
    from orb.core.collection import Collection

    collection = Collection(model=MockUser)
    collection.preload_data({'first': mock_user_data})

    record = collection.first()
    assert record is not None
    assert record.get('username') == mock_user_data['username']


def test_retrieve_first_record_from_backend(mock_user_data, mock_db, MockUser):
    from orb.core.collection import Collection

    checks = {}

    def select_method(model, context):
        checks['limit_one'] = context.limit == 1
        checks['ordered'] = context.order == [('id', 'asc')]
        return [mock_user_data]

    db = mock_db(responses={
        'select': select_method
    })

    collection = Collection(model=MockUser, db=db)
    record = collection.first()
    assert record is not None
    assert record.get('username') == mock_user_data['username']
    assert checks['limit_one']
    assert checks['ordered']


def test_group_records_with_preloading(MockUser):
    from orb.core.collection import Collection

    users = [
        MockUser({'username': 'john.doe', 'first_name': 'John', 'last_name': 'Doe'}),
        MockUser({'username': 'jane.doe', 'first_name': 'Jane', 'last_name': 'Doe'}),
        MockUser({'username': 'john.smith', 'first_name': 'John', 'last_name': 'Smith'})
    ]

    collection = Collection(users)

    group_by_last_name = collection.grouped('last_name')
    group_by_first_name = collection.grouped('first_name')

    assert len(group_by_last_name['Doe']) == 2
    assert len(group_by_last_name['Smith']) == 1
    assert len(group_by_first_name['John']) == 2
    assert len(group_by_first_name['Jane']) == 1

    assert group_by_last_name['Doe'].first() == users[0]
    assert group_by_last_name['Doe'].last() == users[1]
    assert group_by_last_name['Smith'].first() == users[2]

    group_by_last_first_name = collection.grouped('last_name', 'first_name')

    assert group_by_last_first_name['Doe']['John'].first() == users[0]
    assert group_by_last_first_name['Doe']['Jane'].first() == users[1]
    assert group_by_last_first_name['Smith']['John'].first() == users[2]


def test_group_records_without_preloading(mock_db, MockUser):
    import orb
    from orb.core.collection import Collection

    db = mock_db(responses={
        'select': (
            {'last_name': 'Doe'},
            {'last_name': 'Smith'}
        )
    })

    collection = Collection(model=MockUser, db=db)

    grouped_by_last_name = collection.grouped('last_name')

    doe_json = (orb.Query('last_name') == 'Doe').__json__()
    smith_json = (orb.Query('last_name') == 'Smith').__json__()

    assert len(grouped_by_last_name) == 2

    assert isinstance(grouped_by_last_name['Doe'], Collection)
    assert isinstance(grouped_by_last_name['Smith'], Collection)

    assert grouped_by_last_name['Doe'].context().where.__json__() == doe_json
    assert grouped_by_last_name['Smith'].context().where.__json__() == smith_json

    db = mock_db(responses={
        'select': (
            {'last_name': 'Doe', 'first_name': 'John'},
            {'last_name': 'Doe', 'first_name': 'Jane'},
            {'last_name': 'Smith', 'first_name': 'John'}
        )
    })

    collection = Collection(model=MockUser, db=db)

    grouped_by_last_first_name = collection.grouped('last_name', 'first_name')

    a = orb.Query('last_name') == 'Doe'
    b = orb.Query('last_name') == 'Smith'
    c = orb.Query('first_name') == 'John'
    d = orb.Query('first_name') == 'Jane'

    ac = a & c
    ad = a & d
    bc = b & c

    assert len(grouped_by_last_first_name) == 2
    assert len(grouped_by_last_first_name['Doe']) == 2
    assert len(grouped_by_last_first_name['Smith']) == 1

    john = grouped_by_last_first_name['Doe']['John']
    jane = grouped_by_last_first_name['Doe']['Jane']
    smith = grouped_by_last_first_name['Smith']['John']

    assert john.context().where.__json__() == ac.__json__()
    assert jane.context().where.__json__() == ad.__json__()
    assert smith.context().where.__json__() == bc.__json__()


def test_record_inclusion_in_collection_using_cache(MockUser):
    from orb.core.collection import Collection

    a = MockUser()
    b = MockUser()
    c = MockUser()

    collection = Collection([a, b])

    assert collection.has(a)
    assert collection.has(b)
    assert not collection.has(c)


def test_record_inclusion_in_collection_using_count(mock_db, MockUser):
    from orb.core.collection import Collection

    user = MockUser()

    with_db = mock_db(responses={
        'count': 1
    })
    without_db = mock_db(responses={
        'count': 0
    })

    without_collection = Collection(model=MockUser, db=without_db)
    with_collection = Collection(model=MockUser, db=with_db)

    assert not without_collection.has(user)
    assert with_collection.has(user)


def test_collection_fetch_ids_from_null():
    from orb.core.collection import Collection

    collection = Collection()
    assert collection.ids() == []


def test_collection_preloaded_with_preloaded_ids():
    from orb.core.collection import Collection

    collection = Collection()
    collection.preload_data({'ids': [1]})

    assert collection.ids() == [1]


def test_collection_ids_with_queried_data(mock_db, MockUser):
    from orb.core.collection import Collection

    db = mock_db(responses={
        'select': ({'id': 1}, {'id': 2})
    })

    collection = Collection(model=MockUser, db=db)
    assert collection.ids() == [1, 2]


def test_find_user_index_from_cache(MockUser):
    from orb.core.collection import Collection

    a = MockUser()
    b = MockUser()
    c = MockUser()

    collection = Collection([a, b])

    assert collection.index(a) == 0
    assert collection.index(b) == 1

    with pytest.raises(ValueError):
        collection.index(c)


def test_find_user_index_from_query(mock_db, MockUser):
    from orb.core.collection import Collection

    db = mock_db(responses={
        'select': ({'id': 1}, {'id': 2})
    })

    collection = Collection(model=MockUser, db=db)

    a = MockUser({'id': 1})
    b = MockUser({'id': 2})
    c = MockUser({'id': 3})

    assert collection.index(a) == 0
    assert collection.index(b) == 1
    with pytest.raises(ValueError):
        collection.index(c)


def test_retrieve_last_record_from_null():
    from orb.core.collection import Collection

    collection = Collection()
    assert collection.last() is None


def test_retrieve_last_record_from_cache(MockUser):
    from orb.core.collection import Collection

    users = [
        MockUser(),
        MockUser(),
        MockUser()
    ]

    collection = Collection(users)
    assert collection.last() == users[-1]


def test_retrieve_last_record_from_preloaded_cache(mock_user_data, MockUser):
    from orb.core.collection import Collection

    collection = Collection(model=MockUser)
    collection.preload_data({'last': mock_user_data})
    assert collection.last().get('username') == mock_user_data['username']


def test_retrieve_last_record_from_query(mock_db, MockUser):
    from orb.core.collection import Collection

    db = mock_db(responses={
        'select': ({'id': 1},)
    })

    collection = Collection(model=MockUser, db=db)
    assert collection.last().get('id') == 1

    collection = Collection(model=MockUser, db=db, order='-username')
    assert collection.last().get('id') == 1


def test_modifying_order_of_collecction(mock_user_collection):
    collection = mock_user_collection(2)
    assert collection.context().order is None

    collection2 = collection.ordered('-id')
    assert collection2.context().order == [('id', 'desc')]


def test_page_without_size_returns_duplicate(mock_user_collection):
    collection = mock_user_collection(3)
    assert len(collection.page(1, page_size=1)) == 1
    assert len(collection.page(1)) == 3

    collection = mock_user_collection(20, page_size=10)
    assert len(collection.page(1)) == 10


def test_get_page_count(mock_db, mock_user_collection):
    from orb.core.collection import Collection

    collection = Collection()
    assert collection.page_count() == 1
    assert collection.page_count(page_size=10) == 1

    collection = mock_user_collection(10)

    assert collection.page_count() == 1
    assert collection.page_count(page_size=5) == 2

    db = mock_db(responses={
        'count': 10
    })

    collection = mock_user_collection(10, page_size=5, db=db)
    assert collection.page_count() == 2


def test_fetch_null_records():
    from orb.core.collection import Collection

    collection = Collection()
    assert collection.records() == []

    for r in collection.iter_records():
        assert False

    with pytest.raises(StopIteration):
        collection.iter_records().next()


def test_collection_refining():
    from orb.core.collection import Collection

    collection = Collection()
    assert collection.context().columns is None

    collection2 = collection.refine(columns=['id'])
    assert collection.context().columns is None
    assert collection2.context().columns == ['id']

    collection.refine(create_new=False, columns=['username'])
    assert collection.context().columns == ['username']
    assert collection2.context().columns == ['id']


def test_removing_record_from_collection(MockUser):
    from orb.core.collection import Collection

    a = MockUser()
    b = MockUser()
    c = MockUser()

    collection = Collection([a, b, c])
    collection.remove(b)
    assert list(collection) == [a, c]

    with pytest.raises(ValueError):
        collection.remove(b)


def test_removing_record_from_null_collection():
    from orb.core.collection import Collection

    collection = Collection()
    with pytest.raises(ValueError):
        collection.remove(None)


def test_reversing_collection():
    from orb.core.collection import Collection

    collection = Collection(order='+id,-username')
    assert collection.context().order == [('id', 'asc'), ('username', 'desc')]

    rev_collection = collection.reversed()
    assert rev_collection.context().order == [('id', 'desc'), ('username', 'asc')]


def test_reversing_collection_from_cache(MockUser):
    from orb.core.collection import Collection

    a = MockUser()
    b = MockUser()
    c = MockUser()

    collection = Collection([a, b, c])
    assert list(collection) == [a, b, c]

    rev_collection = collection.reversed()
    assert list(rev_collection) == [c, b, a]


def test_removing_record_from_collector(MockUser):
    from orb.core.collection import Collection

    class MockCollector(object):
        def __init__(self, a, b):
            self.a = a
            self.b = b
            self.deleted = False

        def remove_record(self, source_record, target_record, **context):
            assert source_record == self.a
            assert target_record == self.b
            self.deleted = True

    a = MockUser()
    b = MockUser()

    collector = MockCollector(a, b)
    collection = Collection().bind_source_record(a).bind_collector(collector)

    assert collector.deleted is False
    collection.remove(b)
    assert collector.deleted is True


def test_update_collection_using_collector(MockUser):
    from orb.core.collection import Collection

    class MockCollector(object):
        def __init__(self, a):
            self.source = a
            self.updated = False

        def settermethod(self):
            def _setter(source_record, records, **context):
                assert source_record == self.source
                self.updated = True
            return _setter

    a = MockUser()
    collector = MockCollector(a)
    collection = Collection().bind_source_record(a).bind_collector(collector)
    collection.update([])

    assert collector.updated


def test_update_collection_from_records(MockUser):
    from orb.core.collection import Collection

    a = MockUser()
    b = MockUser()
    c = MockUser()

    collection = Collection()
    collection.update([a, b, c])

    assert list(collection) == [a, b, c]


def test_update_collection_from_other_collection(MockUser):
    from orb.core.collection import Collection

    a = MockUser()
    b = MockUser()
    c = MockUser()

    first = Collection([a, b, c])
    second = Collection()

    second.update(first)

    assert list(second) == [a, b, c]


def test_update_collection_from_invalid_options():
    import orb
    from orb.core.collection import Collection

    collection = Collection()
    with pytest.raises(orb.errors.OrbError):
        collection.update(1)


def test_update_collection_from_data(mock_db, MockUser):
    import orb
    from orb.core.collection import Collection

    collection = Collection()
    with pytest.raises(orb.errors.OrbError):
        collection.update([{'id': 1}])

    db = mock_db(responses={
        'select': ({'id': 1, 'username': 'jdoe'},)
    })

    collection = Collection(model=MockUser, db=db)
    collection.update([{'id': 1}])

    assert len(collection) == 1
    assert collection.at(0).get('username') == 'jdoe'

    collection = Collection(model=MockUser, db=db)
    collection.update({'ids': [{'id': 1}]})
    assert len(collection) == 1
    assert collection.at(0).get('username') == 'jdoe'

    collection = Collection(model=MockUser, db=db)
    collection.update({'records': [{'id': 1}]})
    assert len(collection) == 1
    assert collection.at(0).get('username') == 'jdoe'

    collection = Collection(model=MockUser, db=db)
    with pytest.raises(orb.errors.OrbError):
        collection.update({})


def test_update_collection_with_record_save(mock_db, MockUser):
    from orb.core.collection import Collection

    checks = {}
    def save(record, **context):
        checks['saved'] = True

    MockUser.save = save

    db = mock_db(responses={
        'select': ({'id': 1, 'username': 'jdoe'},)
    })

    collection = Collection(model=MockUser, db=db)
    collection.update([{'id': 1, 'username': 'john.doe'}])

    assert checks.get('saved')
    assert len(collection) == 1
    assert collection.at(0).get('username') == 'john.doe'


def test_update_collection_with_record_creation(mock_db, MockUser):
    from orb.core.collection import Collection

    def create_record(cls, values, **context):
        values['id'] = 100
        output = cls(values)
        output.mark_loaded()
        return output

    MockUser.create = classmethod(create_record)

    collection = Collection(model=MockUser)
    collection.update([{'username': 'jdoe'}])
    assert len(collection) == 1
    assert collection.at(0).get('id') == 100
    assert collection.at(0).get('username') == 'jdoe'


def test_update_collection_with_record_creation_from_collector(mock_db, MockUser):
    from orb.core.collection import Collection

    class MockCollector(object):
        def __init__(self, source, collection):
            self.source = source
            self.collection = collection
            self.created = False
            self.created_record = None
            self.updated = False

        def create_record(self, source_record, attrs, **context):
            assert self.source == source_record
            attrs['id'] = 100
            user = MockUser(attrs)
            user.mark_loaded()
            context = self.collection.context()
            self.created_record = user
            return user

        def update_records(self,
                           source_record,
                           records,
                           **context):
            assert source_record == self.source
            self.updated = True

    a = MockUser()

    collection = Collection(model=MockUser)
    collector = MockCollector(a, collection)
    collection.bind_source_record(a).bind_collector(collector)
    collection.update([{'username': 'john.doe'}])
    assert collector.created_record.get('id') == 100
    assert collector.created_record.get('username') == 'john.doe'


def test_save_collection(mock_db, MockUser):
    from orb.core.collection import Collection

    db = mock_db(responses={
        'select': ({'id': 1, 'username': 'jdoe'},),
        'insert': (({'id': 2, 'username': 'jane.doe'},), 1)
    })

    a = MockUser(1, db=db)
    b = MockUser({'username': 'jane.doe'})

    collection = Collection([a, b], db=db)
    collection.save()

    assert collection.at(0).get('id') == 1
    assert collection.at(1).get('id') == 2

    collection = Collection()
    collection.save()
    assert len(collection) == 0


def test_extract_values_from_null_collection():
    from orb.core.collection import Collection

    collection = Collection()
    assert collection.values() == []

    for v in collection.iter_values():
        assert False

    with pytest.raises(StopIteration):
        collection.iter_values().next()


def test_expand_reference_values(mock_db, MockUser, MockGroup):
    from orb.core.collection import Collection

    def select_data(model, context):
        if model == MockUser:
            return ({'id': 1, 'username': 'jdoe', 'group_id': 1},)
        else:
            return ({'id': 1, 'name': 'testing-group'},)

    db = mock_db(responses={'select': select_data})
    collection = Collection(model=MockUser, db=db)
    collection.preload_data({'id': 1, 'group_id': 1})

    groups = collection.values('group')
    assert len(groups) == 1
    assert isinstance(groups[0], MockGroup)
    assert groups[0].get('name') == 'testing-group'

    group_and_names = collection.values('group', 'username')
    assert len(group_and_names) == 1
    assert isinstance(group_and_names[0], tuple)
    assert isinstance(group_and_names[0][0], MockGroup)
    assert group_and_names[0][0].get('name') == 'testing-group'

    name_and_groups = collection.values('username', 'group')
    assert len(name_and_groups) == 1
    assert isinstance(name_and_groups[0], tuple)
    assert isinstance(name_and_groups[0][1], MockGroup)
    assert name_and_groups[0][1].get('name') == 'testing-group'

    name_and_group_ids = collection.values('username', 'group_id')
    assert len(name_and_group_ids) == 1
    assert isinstance(name_and_group_ids[0], tuple)
    assert name_and_group_ids[0] == ('jdoe', 1)


def test_values_from_cache(mock_user_collection):
    collection = mock_user_collection(3)

    assert collection.values('username') == ['jdoe'] * 3
    assert collection.values('first_name', 'username') == [('John', 'jdoe')] * 3


def test_search():
    import orb

    class TestModel(orb.Table):
        __register__ = False

        id = orb.IdColumn()
        name = orb.StringColumn(flags={'Searchable'})

    records = orb.Collection(model=TestModel)
    search_records = records.search('testing')

    q = search_records.context().where.__json__()
    validate_q = orb.Query(TestModel, 'name').asString().matches('(^|.*\s)testing', caseSensitive=False)

    assert q == validate_q.__json__()


def test_deprecated_methods(mock_db, mock_user_collection):
    db = mock_db(responses={
        'count': 0
    })
    collection = mock_user_collection(db=db)
    collection.clear()

    assert collection.isEmpty()
    assert not collection.isLoaded()
    assert not collection.isNull()
    assert collection.pageCount() == 1


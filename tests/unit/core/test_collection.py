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
            records = [MockUser(mock_user_data) for _ in xrange(count)]
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

    assert collection.page_count(pageSize=10) == 10
    assert count == 100


def test_batch_iterator_from_helper_method(mock_user_collection):
    collection = mock_user_collection(100)

    count = 0
    for _ in collection.iterate(batch=10):
        count += 1

    assert collection.page_count(pageSize=10) == 10
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
    collection.add_preloaded_data({'records': [mock_user_data]})

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
    collection.add_preloaded_data({'records': [mock_user_data]})
    coll_copy = collection.copy()
    assert len(coll_copy) == 1


def test_null_collection_length_is_zero():
    from orb.core.collection import Collection

    collection = Collection()
    assert len(collection) == 0


def test_collection_with_preloaded_count():
    from orb.core.collection import Collection

    collection = Collection()
    collection.add_preloaded_data({'count': 3})

    assert len(collection) == 3


def test_collection_count_with_preloaded_records(mock_user_data):
    from orb.core.collection import Collection

    collection = Collection()
    collection.add_preloaded_data({'records': [mock_user_data]})

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
    class MockCollector(object):
        def __init__(self):
            self.deleted = False

        def delete_records(self, collection, **context):
            self.deleted = True
            return list(collection), len(collection)

    collection = mock_user_collection(3)
    collector = MockCollector()

    collection.bind_collector(collector)

    assert collection.delete() == 3
    assert collector.deleted is True


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
    collection.add_preloaded_data({'first': mock_user_data})

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
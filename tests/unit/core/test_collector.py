import pytest

def test_basic_collector():
    from orb.core.collector import Collector

    collector = Collector(name='testing', flags={'Unique'})

    assert collector.__name__ == 'testing'
    assert collector.name() == 'testing'
    assert collector(None).is_null()
    assert collector.test_flag(Collector.Flags.Unique) is True
    assert collector.test_flag(Collector.Flags.AutoExpand) is False


def test_collector_ordering():
    from orb.core.collector import Collector

    a = Collector(name='alpha')
    b = Collector(name='bravo')
    c = Collector(name='charlie')

    d = [b, c, a]
    d.sort()

    assert a == a
    assert a != b
    assert d == [a, b, c]
    assert (a < a) is False
    assert (a < Collector(name='alpha')) is False
    assert (a > Collector(name='alpha')) is False
    assert (a < 10) is False


def test_abstract_collector_methods():
    from orb.core.collector import Collector

    collector = Collector()
    with pytest.raises(NotImplementedError):
        collector._collect(None)
    with pytest.raises(NotImplementedError):
        collector.add_record(None, None)
    with pytest.raises(NotImplementedError):
        collector.collect_expand(None, [])
    with pytest.raises(NotImplementedError):
        collector.create_record(None, {})
    with pytest.raises(NotImplementedError):
        collector.delete_records(None, [])
    with pytest.raises(NotImplementedError):
        collector.remove_record(None, None)
    with pytest.raises(NotImplementedError):
        collector.update_records(None, [])


def test_collector_serialization(MockUser):
    from orb.core.collector import Collector

    a = Collector(name='testing', flags={'Unique'})
    b = Collector(name='testing', model=MockUser, flags=1)
    c = Collector(name='testing', model='User')

    assert a.__json__() == {'name': 'testing',
                            'model': None,
                            'flags': {'Unique': True}}
    assert b.__json__() == {'name': 'testing',
                            'model': 'MockUser',
                            'flags': {'Unique': True}}

    assert c.__json__() == {'name': 'testing',
                            'model': None,
                            'flags': {}}


def test_collector_callable_not_implemented(MockUser):
    from orb.core.collector import Collector

    collector = Collector()

    source_record = MockUser({'id': 1})
    source_record.mark_loaded()

    assert source_record.is_record()

    with pytest.raises(NotImplementedError):
        collector(source_record)


def test_collector_filter_wrapper():
    from orb.core.query import Query as Q
    from orb.core.collector import Collector

    checks = {}

    def build_filter(q, **context):
        checks['filtered'] = True
        assert isinstance(q, Q)
        return q

    a = Collector(filter=build_filter)
    b = Collector()
    b.filter(build_filter)
    c = Collector()
    c.filter()(build_filter)

    assert a.filtermethod() == build_filter
    assert b.filtermethod() == build_filter
    assert c.filtermethod() == build_filter

    assert a.filtermethod()(Q('collector') == True)
    assert checks['filtered'] is True


def test_collector_getter_wrapper():
    from orb.core.collection import Collection
    from orb.core.collector import Collector

    checks = {}

    def get_records(source_record, **context):
        checks['fetched'] = True
        return Collection()

    a = Collector(getter=get_records)
    b = Collector()
    b.getter(get_records)
    c = Collector()
    c.getter()(get_records)

    assert a.gettermethod() == get_records
    assert b.gettermethod() == get_records
    assert c.gettermethod() == get_records

    assert checks.get('fetched') is None
    assert a.gettermethod()(None).is_null()
    assert checks.pop('fetched') is True

    assert a(None).is_null()
    assert checks.pop('fetched') is True


def test_collector_setter_wrapper():
    from orb.core.collection import Collection
    from orb.core.collector import Collector

    checks = {}

    def set_records(source_record, records, **context):
        checks['set'] = True
        return Collection()

    a = Collector(setter=set_records)
    b = Collector()
    b.setter(set_records)
    c = Collector()
    c.setter()(set_records)

    assert a.settermethod() == set_records
    assert b.settermethod() == set_records
    assert c.settermethod() == set_records

    assert checks.get('set') is None
    assert a.settermethod()(None, []).is_null()
    assert checks.pop('set') is True

def test_collector_collection_method(mock_db, MockUser):
    from orb.core.context import Context
    from orb.core.collection import Collection
    from orb.core.collector import Collector

    class MockCollector(Collector):
        def __init__(self, response, **kw):
            super(MockCollector, self).__init__(**kw)

            self.response = response

        def _collect(self, source_record, **records):
            return self.response

    without_preload = Collection([MockUser({'username': 'jdoe'})])
    with_preload = Collection(model=MockUser)

    a = MockCollector(without_preload)
    b = MockCollector(with_preload, name='testing')
    c = MockCollector(without_preload, name='testing', flags={'Unique'})
    d = MockCollector([{'username': 'jdoe'}])
    e = MockCollector(MockUser({'username': 'jdoe'}))

    # create a source record for the collection
    source = MockUser({'id': 1})
    source.mark_loaded()
    source.preload_data({'testing': [{'username': 'jdoe'}]})

    a_records = a(source)
    b_records = b(source)
    c_records = c(source)
    d_records = d(source)
    e_records = e(source)

    assert isinstance(a_records, Collection)
    assert isinstance(b_records, Collection)
    assert isinstance(c_records, MockUser)
    assert isinstance(d_records, list)
    assert isinstance(e_records, MockUser)

    assert a_records.at(0).get('username') == 'jdoe'
    assert b_records.at(0).get('username') == 'jdoe'
    assert c_records.get('username') == 'jdoe'
    assert d_records[0].get('username') == 'jdoe'
    assert e_records.get('username') == 'jdoe'